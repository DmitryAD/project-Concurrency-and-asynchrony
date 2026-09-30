"""
Сквозная проверка всех дней проекта.

    python -m tests.check_all

Зачем: каждый новый день меняет общий код в crawler/. Эта проверка
ловит момент, когда новая логика незаметно сломала старую. Запускай
её после КАЖДОГО изменения, а с появлением нового дня дописывай сюда
его проверки.

Интернет не нужен. Скрипт поднимает на твоём компьютере два маленьких
HTTP-сервера (127.0.0.1, случайные порты) и гоняет по ним настоящий
aiohttp. Серверы сами считают:
  - сколько запросов пришло к ним одновременно (пик),
  - сколько раз запросили каждую страницу.
Так лимиты и отсутствие дублей проверяются со стороны сервера,
а не по отчёту самого краулера.

Итог — список проверок с ✓/✗ и код выхода: 0, если прошло всё, 1 — если нет.
"""

from __future__ import annotations

import asyncio
import logging
import socket
import sys
import threading
import time
import traceback
from collections import Counter, defaultdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from crawler import AsyncCrawler, CrawlerQueue, HTMLParser


# ============================================================
# Локальный тестовый сайт
# ============================================================

def page(title: str, *links: str, extra: str = "") -> str:
    anchors = "".join(f'<a href="{link}">{link}</a>' for link in links)
    return (f"<html><head><title>{title}</title></head>"
            f"<body>{extra}{anchors}</body></html>")


# Сайт для обхода (день 3). Внутри есть цикл (/a/2 → /), повторные
# ссылки (/b → /a), несуществующая страница (/c) и служебный раздел.
# Ссылку на «чужой домен» подставляем позже — это адрес второго сервера.
SITE = {
    "/site/": ["/site/a", "/site/b", "/site/c", "/site/private/login", "EXTERNAL", "/site/a"],
    "/site/a": ["/site/a/1", "/site/a/2", "/site/"],
    "/site/b": ["/site/b/1", "/site/a"],
    "/site/private/login": [],
    "/site/a/1": ["/site/a/1/deep"],
    "/site/a/2": ["/site/"],
    "/site/b/1": ["/site/b/1/deep"],
    "/site/a/1/deep": [],
    "/site/b/1/deep": [],
    "/site/x": [],
}

# Страница для проверки разбора (день 2)
PARSE_PAGE = (
    '<html><head><title>Разбор</title>'
    '<script>var secret = "не должно попасть в текст";</script></head>'
    '<body><h1>Заголовок</h1><a href="/about">о нас</a>'
    '<a href="sub/page.html">вложенная</a><a href="mailto:x@y.z">почта</a>'
    '</body></html>'
)


class Stats:
    """Что видит сервер. Общая на оба сервера, защищена замком."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        self.inflight_total = 0
        self.peak_total = 0
        self.inflight = defaultdict(int)     # порт -> сейчас
        self.peak = defaultdict(int)         # порт -> пик
        self.hits = Counter()                # (порт, путь) -> сколько раз


STATS = Stats()


class Handler(BaseHTTPRequestHandler):
    external = ""   # адрес второго сервера, подставляется при старте

    def log_message(self, *args) -> None:   # не шуметь в консоль
        pass

    def do_GET(self) -> None:
        port = self.server.server_address[1]
        path = self.path.split("?", 1)[0]    # хвост ?n=1 не нужен для маршрута

        # /hang/N — для проверки таймаута. Клиент бросает такой запрос,
        # а сервер ещё несколько секунд «досыпает». Чтобы этот хвост не
        # испортил подсчёт в следующих проверках, его не считаем вовсе.
        if path.startswith("/hang/"):
            try:
                time.sleep(float(path.rsplit("/", 1)[1]))
                self._send(200, page("Долго"))
            except (BrokenPipeError, ConnectionResetError):
                pass
            return

        with STATS.lock:
            STATS.hits[(port, path)] += 1
            STATS.inflight[port] += 1
            STATS.inflight_total += 1
            STATS.peak[port] = max(STATS.peak[port], STATS.inflight[port])
            STATS.peak_total = max(STATS.peak_total, STATS.inflight_total)

        # Сначала «работаем» и снимаемся с учёта, и только ПОТОМ отвечаем.
        # Если ответить раньше, клиент получит ответ, следующая проверка
        # успеет обнулить статистику, а наше уменьшение счётчика прилетит
        # уже после — и испортит подсчёт пика в чужой проверке.
        try:
            code, body = self._route(path)
        finally:
            with STATS.lock:
                STATS.inflight[port] -= 1
                STATS.inflight_total -= 1

        try:
            self._send(code, body)
        except (BrokenPipeError, ConnectionResetError):
            pass   # клиент ушёл раньше — это не ошибка сервера

    def _send(self, code: int, body: str) -> None:
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _route(self, path: str) -> tuple[int, str]:
        """Готовит ответ: (код, тело). Отправляет его do_GET."""
        if path == "/ok":
            return 200, page("OK")
        if path.startswith("/delay/"):
            time.sleep(float(path.rsplit("/", 1)[1]))
            return 200, page("Задержка")
        if path.startswith("/status/"):
            code = int(path.rsplit("/", 1)[1])
            return code, page(f"Код {code}")
        if path == "/parse":
            return 200, PARSE_PAGE
        if path in SITE and path != "/site/c":
            links = [self.external if link == "EXTERNAL" else link for link in SITE[path]]
            return 200, page(path, *links)
        return 404, page("Не найдено")


class TestServer(ThreadingHTTPServer):
    daemon_threads = True
    # Очередь входящих подключений. По умолчанию в Python она на 5 мест:
    # если краулер откроет 8 соединений разом, а сервер не успеет их
    # принять, лишние ядро отбросит и клиент повторит их только через
    # секунду. Тогда проверка лимита покажет пик 6 вместо 8 — ошибка
    # теста, а не краулера. 128 мест хватит с запасом.
    request_queue_size = 128


def start_server() -> tuple[ThreadingHTTPServer, str]:
    server = TestServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


def closed_port() -> int:
    """Порт, на котором точно никто не слушает."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ============================================================
# Проверки
# ============================================================

CHECKS: list[tuple[str, str, object]] = []


def check(day: str, name: str):
    def register(fn):
        CHECKS.append((day, name, fn))
        return fn
    return register


def port_of(base: str) -> int:
    return int(base.rsplit(":", 1)[1])


# ---------- День 1 ----------

@check("День 1", "fetch_url возвращает HTML")
async def _(B, B2):
    c = AsyncCrawler()
    try:
        html = await c.fetch_url(f"{B}/ok")
    finally:
        await c.close()
    assert html and "<title>OK</title>" in html, html
    assert c.successful == 1 and c.failed == 0


@check("День 1", "404 и 500 не роняют программу, причина записана")
async def _(B, B2):
    c = AsyncCrawler()
    try:
        r1 = await c.fetch_url(f"{B}/status/404")
        r2 = await c.fetch_url(f"{B}/status/500")
    finally:
        await c.close()
    assert r1 is None and r2 is None
    assert "HTTP 404" in c.errors[f"{B}/status/404"], c.errors
    assert "HTTP 500" in c.errors[f"{B}/status/500"], c.errors


@check("День 1", "недоступный сервер не роняет программу")
async def _(B, B2):
    url = f"http://127.0.0.1:{closed_port()}/"
    c = AsyncCrawler()
    try:
        r = await c.fetch_url(url)
    finally:
        await c.close()
    assert r is None and url in c.errors, c.errors


@check("День 1", "таймаут обрывает запрос вовремя")
async def _(B, B2):
    c = AsyncCrawler(total_timeout=0.5)
    t = time.perf_counter()
    try:
        r = await c.fetch_url(f"{B}/hang/3")
    finally:
        await c.close()
    dt = time.perf_counter() - t
    assert r is None and "TimeoutError" in c.errors[f"{B}/hang/3"], c.errors
    assert dt < 1.5, f"ждали {dt:.2f} c вместо ~0.5"


@check("День 1", "параллельно быстрее последовательного")
async def _(B, B2):
    urls = [f"{B}/delay/0.3?n={i}" for i in range(5)]
    c = AsyncCrawler(max_concurrent=10)
    try:
        t = time.perf_counter()
        for u in urls:
            await c.fetch_url(u)
        seq = time.perf_counter() - t
        t = time.perf_counter()
        await c.fetch_urls(urls)
        par = time.perf_counter() - t
    finally:
        await c.close()
    assert seq > 1.4 and par < 0.9, f"последовательно {seq:.2f} c, параллельно {par:.2f} c"


@check("День 1", "max_concurrent соблюдается (пик на сервере = 3)")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_concurrent=3)
    try:
        await c.fetch_urls([f"{B}/delay/0.2?n={i}" for i in range(9)])
    finally:
        await c.close()
    assert STATS.peak[port_of(B)] == 3, f"пик {STATS.peak[port_of(B)]}"


@check("День 1", "без лимита на домен работает только max_concurrent (пик = 8)")
async def _(B, B2):
    # Регресс: день 3 не должен молча урезать лимит первого дня
    STATS.reset()
    c = AsyncCrawler(max_concurrent=8)
    t = time.perf_counter()
    try:
        await c.fetch_urls([f"{B}/delay/0.3?n={i}" for i in range(8)])
    finally:
        await c.close()
    dt = time.perf_counter() - t
    assert STATS.peak[port_of(B)] == 8, (
        f"пик {STATS.peak[port_of(B)]}, ожидали 8; заняло {dt:.2f} c (норма ~0.3)")


@check("День 1", "fetch_urls возвращает только успешные страницы")
async def _(B, B2):
    c = AsyncCrawler()
    try:
        pages = await c.fetch_urls([f"{B}/ok", f"{B}/status/404", f"{B}/delay/0.1"])
    finally:
        await c.close()
    assert set(pages) == {f"{B}/ok", f"{B}/delay/0.1"}, list(pages)
    assert c.successful == 2 and c.failed == 1


# ---------- День 2 ----------

@check("День 2", "fetch_and_parse: заголовок, абсолютные ссылки, без JavaScript")
async def _(B, B2):
    c = AsyncCrawler()
    try:
        d = await c.fetch_and_parse(f"{B}/parse")
    finally:
        await c.close()
    assert d["error"] is None
    assert d["title"] == "Разбор", d["title"]
    assert d["links"] == [f"{B}/about", f"{B}/sub/page.html"], d["links"]
    assert "secret" not in d["text"], d["text"]
    assert d["headings"]["h1"] == ["Заголовок"]


@check("День 2", "fetch_and_parse на 404: тот же словарь плюс error")
async def _(B, B2):
    c = AsyncCrawler()
    try:
        ok = await c.fetch_and_parse(f"{B}/parse")
        bad = await c.fetch_and_parse(f"{B}/status/404")
    finally:
        await c.close()
    assert set(ok) == set(bad), set(ok) ^ set(bad)
    assert "HTTP 404" in bad["error"] and bad["links"] == []


@check("День 2", "для разбора используется lxml")
async def _(B, B2):
    assert HTMLParser().parser == "lxml", "lxml не установлен — pip install -r requirements.txt"


@check("День 2", "битый HTML разбирается точно")
async def _(B, B2):
    broken = """<html><head><title>Битая</title><body>
        <h1>Заголовок без закрытия
        <p>Абзац <a href=/no-quotes>без кавычек</a>
        <table><tr><td>Ячейка<tr><td>Вторая строка</table>
        <ul><li>Один<li>Два</ul>"""
    d = await HTMLParser().parse_html(broken, "https://b.test/")
    assert d["parse_errors"] == []
    assert d["title"] == "Битая"
    assert d["headings"]["h1"] == ["Заголовок без закрытия"], d["headings"]["h1"]
    assert d["links"] == ["https://b.test/no-quotes"], d["links"]
    assert d["tables"] == [[["Ячейка"], ["Вторая строка"]]], d["tables"]
    assert d["lists"] == [["Один", "Два"]], d["lists"]


@check("День 2", "относительные ссылки превращаются в абсолютные")
async def _(B, B2):
    html = "".join(f'<a href="{h}">x</a>' for h in (
        "/root", "sib.html", "../up.html", "./same.html", "//cdn.test/x",
        "#anchor", "mailto:a@b.c", "tel:1", "javascript:void(0)", "/root", "/p#frag",
    ))
    d = await HTMLParser().parse_html(html, "https://s.test/cat/sub/page.html")
    assert d["links"] == [
        "https://s.test/root",
        "https://s.test/cat/sub/sib.html",
        "https://s.test/cat/up.html",
        "https://s.test/cat/sub/same.html",
        "https://cdn.test/x",
        "https://s.test/p",
    ], d["links"]


# ---------- День 3 ----------

@check("День 3", "очередь: приоритеты и отказ от дубликатов")
async def _(B, B2):
    q = CrawlerQueue()
    q.add_url("https://q.test/low", priority=0)
    q.add_url("https://q.test/high", priority=10)
    q.add_url("https://q.test/mid", priority=5)
    q.add_url("https://q.test/high2", priority=10)
    assert q.add_url("https://q.test/mid", priority=99) is False
    order = [await q.get_next() for _ in range(4)]
    assert [u.rsplit("/", 1)[1] for u in order] == ["high", "high2", "mid", "low"], order


@check("День 3", "глубина обхода ограничивается")
async def _(B, B2):
    c = AsyncCrawler(max_depth=1)
    try:
        res = await c.crawl([f"{B}/site/"], same_domain_only=True)
    finally:
        await c.close()
    paths = sorted(u.replace(B, "") for u in c.visited_urls)
    assert paths == ["/site/", "/site/a", "/site/b", "/site/c", "/site/private/login"], paths
    assert max(d["depth"] for d in res.values()) == 1


@check("День 3", "каждая страница запрошена сервером ровно один раз")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_depth=5)
    try:
        await c.crawl([f"{B}/site/"], same_domain_only=True)
    finally:
        await c.close()
    hits = {path: n for (port, path), n in STATS.hits.items() if port == port_of(B)}
    repeats = {p: n for p, n in hits.items() if n > 1}
    assert not repeats, f"запрошены повторно: {repeats}"
    assert sorted(hits) == [
        "/site/", "/site/a", "/site/a/1", "/site/a/1/deep", "/site/a/2",
        "/site/b", "/site/b/1", "/site/b/1/deep",
        "/site/c",                 # несуществующая — запрошена, получила 404
        "/site/private/login",
    ], sorted(hits)
    assert c.queue_stats["duplicates_rejected"] >= 3


@check("День 3", "фильтры: свой домен, exclude, include")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_depth=3)
    try:
        await c.crawl([f"{B}/site/"], same_domain_only=True, exclude_patterns=[r"/private"])
    finally:
        await c.close()
    assert not any(port == port_of(B2) for port, _ in STATS.hits), "ходили на чужой домен"
    assert not any("/private" in u for u in c.visited_urls)

    c = AsyncCrawler(max_depth=3)
    try:
        await c.crawl([f"{B}/site/"], same_domain_only=True, include_patterns=[r"/site/a"])
    finally:
        await c.close()
    paths = sorted(u.replace(B, "") for u in c.visited_urls)
    assert paths == ["/site/", "/site/a", "/site/a/1", "/site/a/1/deep", "/site/a/2"], paths

    # Без same_domain_only ссылка на второй сервер должна пройти
    STATS.reset()
    c = AsyncCrawler(max_depth=1)
    try:
        await c.crawl([f"{B}/site/"])
    finally:
        await c.close()
    assert STATS.hits[(port_of(B2), "/site/x")] == 1, "без фильтра чужой домен должен обходиться"


@check("День 3", "лимит на домен соблюдается (пик на сервере = 2)")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_concurrent=10, max_per_domain=2)
    try:
        await c.fetch_urls([f"{B}/delay/0.2?n={i}" for i in range(8)])
    finally:
        await c.close()
    assert STATS.peak[port_of(B)] == 2, f"пик {STATS.peak[port_of(B)]}"


@check("День 3", "два сайта: общий лимит 3, на каждый не больше 2")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_concurrent=3, max_per_domain=2)
    urls = [f"{base}/delay/0.2?n={i}" for i in range(6) for base in (B, B2)]
    try:
        await c.fetch_urls(urls)
    finally:
        await c.close()
    assert STATS.peak_total == 3, f"общий пик {STATS.peak_total}"
    assert STATS.peak[port_of(B)] <= 2 and STATS.peak[port_of(B2)] <= 2, dict(STATS.peak)


@check("День 3", "max_pages соблюдается")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_depth=5)
    try:
        await c.crawl([f"{B}/site/"], max_pages=4, same_domain_only=True)
    finally:
        await c.close()
    assert len(c.visited_urls) == 4, len(c.visited_urls)
    assert sum(STATS.hits.values()) == 4, sum(STATS.hits.values())


# ============================================================
# Запуск
# ============================================================

async def run_all() -> int:
    server1, B = start_server()
    server2, B2 = start_server()
    Handler.external = f"{B2}/site/x"

    passed = 0
    current_day = None
    try:
        for day, name, fn in CHECKS:
            if day != current_day:
                print(f"\n{day}")
                current_day = day
            try:
                await fn(B, B2)
                print(f"  ✓ {name}")
                passed += 1
            except AssertionError as e:
                print(f"  ✗ {name}\n      {e}")
            except Exception:
                print(f"  ✗ {name} — неожиданная ошибка:")
                print("      " + traceback.format_exc().strip().replace("\n", "\n      "))
    finally:
        server1.shutdown()
        server2.shutdown()

    total = len(CHECKS)
    print(f"\n{'=' * 50}")
    print(f"  Итого: {passed} из {total} проверок прошли")
    print(f"  {'ВСЁ В ПОРЯДКЕ' if passed == total else 'ЕСТЬ ПРОБЛЕМЫ'}")
    print("=" * 50)
    return 0 if passed == total else 1


def main() -> None:
    # Предупреждения краулера (404, таймауты) здесь ожидаемы и только шумят
    logging.basicConfig(level=logging.CRITICAL)
    sys.exit(asyncio.run(run_all()))


if __name__ == "__main__":
    main()