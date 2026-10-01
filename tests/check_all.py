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
import json
import logging
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import traceback
from collections import Counter, defaultdict
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from crawler import (
    AsyncCrawler, CircuitBreaker, CrawlerQueue, CSVStorage, HTMLParser, JSONStorage,
    RetryStrategy, RobotsParser, SQLiteStorage,
)
from crawler.storage import STANDARD_FIELDS
from crawler.errors import (
    CircuitOpenError, NetworkError, ParseError, PermanentError, RateLimitedError,
    TransientError, classify_exception, classify_status,
)


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

# robots.txt тестового сайта (день 4). Раздаётся обоими серверами.
ROBOTS_TXT = """
User-agent: *
Disallow: /site/private
Crawl-delay: 0.3

User-agent: BlockedBot
Disallow: /
"""

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
        self.starts = defaultdict(list)      # порт -> [(время начала, путь)]
        self.user_agents = []                # заголовки User-Agent по порядку
        self.flaky = Counter()               # ключ капризного адреса -> сколько раз запросили


STATS = Stats()


class Handler(BaseHTTPRequestHandler):
    external = ""   # адрес второго сервера, подставляется при старте

    def log_message(self, *args) -> None:   # не шуметь в консоль
        pass

    def do_GET(self) -> None:
        port = self.server.server_address[1]
        path = self.path.split("?", 1)[0]    # хвост ?n=1 не нужен для маршрута

        # /hang/N и /slowhang/N — для проверок таймаута. Клиент может
        # бросить такой запрос, а сервер ещё досыпает. Чтобы этот хвост
        # не испортил подсчёт пиков в следующих проверках, в «активные»
        # такие запросы не попадают. Число обращений при этом считаем.
        if path.startswith(("/hang/", "/slowhang/")):
            with STATS.lock:
                STATS.hits[(port, path)] += 1
            try:
                time.sleep(float(path.rsplit("/", 1)[1]))
                self._send(200, page("Долго"))
            except (BrokenPipeError, ConnectionResetError):
                pass
            return

        with STATS.lock:
            STATS.hits[(port, path)] += 1
            STATS.starts[port].append((time.monotonic(), path))
            STATS.user_agents.append(self.headers.get("User-Agent", ""))
            STATS.inflight[port] += 1
            STATS.inflight_total += 1
            STATS.peak[port] = max(STATS.peak[port], STATS.inflight[port])
            STATS.peak_total = max(STATS.peak_total, STATS.inflight_total)

        # Сначала «работаем» и снимаемся с учёта, и только ПОТОМ отвечаем.
        # Если ответить раньше, клиент получит ответ, следующая проверка
        # успеет обнулить статистику, а наше уменьшение счётчика прилетит
        # уже после — и испортит подсчёт пика в чужой проверке.
        try:
            code, body, *rest = self._route(path)
            headers = rest[0] if rest else {}
        finally:
            with STATS.lock:
                STATS.inflight[port] -= 1
                STATS.inflight_total -= 1

        try:
            self._send(code, body, headers)
        except (BrokenPipeError, ConnectionResetError):
            pass   # клиент ушёл раньше — это не ошибка сервера

    def _send(self, code: int, body: str, headers: dict | None = None) -> None:
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(data)

    def _route(self, path: str) -> tuple:
        """Готовит ответ: (код, тело) или (код, тело, заголовки)."""
        if path == "/ok":
            return 200, page("OK")
        # День 5. /flaky/<ключ>/<N> — первые N раз отвечает 503, потом 200.
        # /flaky429/<ключ>/<N> — то же с 429 и заголовком Retry-After: 1.
        if path.startswith(("/flaky/", "/flaky429/")):
            kind, key, n = path.strip("/").split("/")
            with STATS.lock:
                STATS.flaky[key] += 1
                count = STATS.flaky[key]
            if count <= int(n):
                if kind == "flaky429":
                    return 429, page("Слишком часто"), {"Retry-After": "1"}
                return 503, page("Перегружен")
            return 200, page("Получилось")
        if path.startswith("/delay/"):
            time.sleep(float(path.rsplit("/", 1)[1]))
            return 200, page("Задержка")
        if path.startswith("/status/"):
            code = int(path.rsplit("/", 1)[1])
            return code, page(f"Код {code}")
        if path == "/parse":
            return 200, PARSE_PAGE
        if path == "/robots.txt":
            return 200, ROBOTS_TXT
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


# ---------- День 4 ----------

def gaps(port: int, skip_robots: bool = True) -> list[float]:
    """Паузы между началами соседних запросов, как их видел сервер."""
    times = [t for t, path in STATS.starts[port] if not (skip_robots and path == "/robots.txt")]
    return [b - a for a, b in zip(times, times[1:])]


EPS = 0.03   # допуск на неточность таймеров


@check("День 4", "один домен: не чаще 5 запросов в секунду")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_concurrent=10, requests_per_second=5)
    t = time.perf_counter()
    try:
        await c.fetch_urls([f"{B}/ok?n={i}" for i in range(6)])
    finally:
        await c.close()
    dt = time.perf_counter() - t
    g = gaps(port_of(B))
    assert len(g) == 5 and min(g) >= 0.2 - EPS, f"паузы {[round(x, 3) for x in g]}"
    assert dt >= 1.0 - EPS, f"6 запросов за {dt:.2f} c, а при 5/с нужно не меньше 1.0"


@check("День 4", "разные домены: у каждого свой лимит")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_concurrent=10, requests_per_second=2)
    t = time.perf_counter()
    try:
        await c.fetch_urls([f"{base}/ok?n={i}" for i in range(3) for base in (B, B2)])
    finally:
        await c.close()
    dt = time.perf_counter() - t
    for port in (port_of(B), port_of(B2)):
        g = gaps(port)
        assert min(g) >= 0.5 - EPS, f"сервер {port}: паузы {[round(x, 3) for x in g]}"
    # Лимиты независимы: два сайта по 3 запроса идут параллельно, ~1.0 c.
    # Будь лимит общим, вышло бы ~2.5 c.
    assert dt < 1.6, f"заняло {dt:.2f} c — похоже, лимит общий, а не по доменам"


@check("День 4", "rate_per_domain=False: один лимит на все домены")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_concurrent=10, requests_per_second=5, rate_per_domain=False)
    t = time.perf_counter()
    try:
        await c.fetch_urls([f"{base}/ok?n={i}" for i in range(3) for base in (B, B2)])
    finally:
        await c.close()
    dt = time.perf_counter() - t
    merged = sorted(t for port in (port_of(B), port_of(B2)) for t, _ in STATS.starts[port])
    g = [b - a for a, b in zip(merged, merged[1:])]
    assert min(g) >= 0.2 - EPS, f"общие паузы {[round(x, 3) for x in g]}"
    assert dt >= 1.0 - EPS, f"6 запросов за {dt:.2f} c"


@check("День 4", "robots.txt разбирается по стандарту")
async def _(B, B2):
    r = RobotsParser()
    r.parse("""
        User-agent: *
        Disallow: /private
        Allow: /private/public
        Disallow: /*.pdf$
        Crawl-delay: 2
        User-agent: MyBot
        Disallow: /mine
    """, "https://r.test/robots.txt")
    expect = {
        ("https://r.test/page", "*"): True,
        ("https://r.test/private/x", "*"): False,
        ("https://r.test/private/public/x", "*"): True,     # длиннее правило побеждает
        ("https://r.test/a/b.pdf", "*"): False,             # * и $
        ("https://r.test/a/b.pdf?v=1", "*"): True,
        ("https://r.test/private/x", "MyBot/1.0"): True,     # своя группа
        ("https://r.test/mine", "MyBot/1.0"): False,
    }
    wrong = {k: r.can_fetch(*k) for k in expect if r.can_fetch(*k) != expect[k]}
    assert not wrong, f"неверно: {wrong}"
    assert r.get_crawl_delay("*", "https://r.test/") == 2.0
    assert r.get_crawl_delay("MyBot/1.0", "https://r.test/") == 0.0


@check("День 4", "запрещённые robots.txt адреса не запрашиваются")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_concurrent=10, max_depth=5, respect_robots=True)
    try:
        await c.crawl([f"{B}/site/"], same_domain_only=True)
    finally:
        await c.close()
    hits = {path: n for (port, path), n in STATS.hits.items() if port == port_of(B)}
    assert not any(p.startswith("/site/private") for p in hits), f"сервер получил: {sorted(hits)}"
    assert c.blocked_urls == {f"{B}/site/private/login"}, c.blocked_urls
    # Запрет — не ошибка: адрес не должен попасть в failed_urls
    # и тратить лимит страниц
    assert f"{B}/site/private/login" not in c.failed_urls, c.failed_urls
    assert c.queue_stats["skipped"] >= 1, c.queue_stats
    assert hits.get("/robots.txt") == 1, f"robots.txt скачан {hits.get('/robots.txt')} раз, а надо 1"
    assert "/site/a/1/deep" in hits, "разрешённые страницы тоже должны обходиться"


@check("День 4", "Crawl-delay из robots.txt соблюдается")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_concurrent=10, max_depth=1, respect_robots=True)
    try:
        await c.crawl([f"{B}/site/"], same_domain_only=True)
    finally:
        await c.close()
    g = gaps(port_of(B))
    assert g and min(g) >= 0.3 - EPS, f"паузы {[round(x, 3) for x in g]}, а Crawl-delay 0.3"


@check("День 4", "правила robots.txt для конкретного User-Agent")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_depth=2, respect_robots=True, user_agent="BlockedBot/1.0")
    try:
        res = await c.crawl([f"{B}/site/"])
    finally:
        await c.close()
    paths = [path for (_, path) in STATS.hits]
    assert paths == ["/robots.txt"], f"сервер получил: {paths}"
    assert res == {} and c.blocked_urls == {f"{B}/site/"}


@check("День 4", "min_delay и jitter: паузы от 0.2 до 0.4 c и не одинаковые")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_concurrent=10, min_delay=0.2, jitter=0.2)
    try:
        await c.fetch_urls([f"{B}/ok?n={i}" for i in range(7)])
    finally:
        await c.close()
    g = gaps(port_of(B))
    assert min(g) >= 0.2 - EPS and max(g) <= 0.4 + EPS, f"паузы {[round(x, 3) for x in g]}"
    assert max(g) - min(g) > 0.02, f"паузы почти одинаковые: {[round(x, 3) for x in g]}"


@check("День 4", "экспоненциальный backoff после ошибок и сброс после успеха")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_concurrent=1, error_backoff=0.2)
    try:
        for path in ("/status/500", "/status/500", "/status/500", "/ok", "/ok?n=2"):
            await c.fetch_url(f"{B}{path}")
    finally:
        await c.close()
    g = gaps(port_of(B))
    # после 1-й ошибки пауза 0.2, после 2-й 0.4, после 3-й 0.8, после успеха 0
    expected = [0.2, 0.4, 0.8, 0.0]
    ok = all(abs(a - e) < 0.12 for a, e in zip(g, expected))
    assert ok and len(g) == 4, f"паузы {[round(x, 3) for x in g]}, ожидали ~{expected}"


@check("День 4", "User-Agent передаётся и ротируется")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(max_concurrent=1, user_agents=["BotA/1.0", "BotB/1.0"])
    try:
        for i in range(4):
            await c.fetch_url(f"{B}/ok?n={i}")
    finally:
        await c.close()
    assert STATS.user_agents == ["BotA/1.0", "BotB/1.0", "BotA/1.0", "BotB/1.0"], STATS.user_agents
    assert c.user_agent == "BotA/1.0", "для robots.txt основное имя — первое в списке"


@check("День 4", "статистика: скорость, средняя пауза, заблокированные")
async def _(B, B2):
    c = AsyncCrawler(max_concurrent=10, max_depth=1, requests_per_second=4, respect_robots=True)
    try:
        await c.crawl([f"{B}/site/"], same_domain_only=True)
    finally:
        await c.close()
    st = c.get_rate_stats()
    # Crawl-delay 0.3 строже, чем 4 запроса/с (0.25), — действует он
    assert abs(st["avg_interval"] - 0.3) < 0.06, st
    assert st["blocked_by_robots"] == 1 and st["total_requests"] == 4, st
    assert st["crawl_delay"] == {f"127.0.0.1:{port_of(B)}": 0.3}, st["crawl_delay"]


# ---------- День 5 ----------

def fast_retry(**kw) -> RetryStrategy:
    """Короткие паузы, чтобы проверки шли быстро."""
    params = dict(max_retries=3, base_delay=0.1, backoff_factor=2.0,
                  base_delay_by_type={}, timeout_growth=1.0)
    params.update(kw)
    return RetryStrategy(**params)


@check("День 5", "ошибки классифицируются правильно")
async def _(B, B2):
    for status in (404, 403, 401):
        assert type(classify_status("u", status, "")) is PermanentError, status
    for status in (500, 502, 503, 504):
        assert type(classify_status("u", status, "")) is TransientError, status
    rl = classify_status("u", 429, "", "7")
    assert isinstance(rl, RateLimitedError) and isinstance(rl, TransientError) and rl.retry_after == 7.0
    assert type(classify_exception(asyncio.TimeoutError(), "u")) is TransientError
    assert type(classify_exception(UnicodeDecodeError("utf-8", b"", 0, 1, "x"), "u")) is ParseError

    c = AsyncCrawler()
    try:
        await c.fetch_url(f"http://127.0.0.1:{closed_port()}/")
        await c.fetch_url(f"{B}/status/404")
    finally:
        await c.close()
    kinds = sorted(d["type"] for d in c.error_details.values())
    assert kinds == ["NetworkError", "PermanentError"], kinds


@check("День 5", "без retry_strategy повторов нет (как в днях 1-4)")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler()
    try:
        r = await c.fetch_url(f"{B}/flaky/norepeat/1")
    finally:
        await c.close()
    assert r is None and STATS.flaky["norepeat"] == 1, STATS.flaky


@check("День 5", "503: повтор и успех со 3-й попытки")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(retry_strategy=fast_retry())
    try:
        html = await c.fetch_url(f"{B}/flaky/k503/2")
    finally:
        await c.close()
    assert html and "Получилось" in html
    assert STATS.flaky["k503"] == 3, f"сервер получил {STATS.flaky['k503']} попыток, ожидали 3"
    assert c.retry_strategy.successful_retries == 1


@check("День 5", "404 и 403 не повторяются")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(retry_strategy=fast_retry())
    try:
        await c.fetch_url(f"{B}/status/404")
        await c.fetch_url(f"{B}/status/403")
    finally:
        await c.close()
    hits = {path: n for (port, path), n in STATS.hits.items()}
    assert hits == {"/status/404": 1, "/status/403": 1}, hits
    assert c.error_details[f"{B}/status/404"]["type"] == "PermanentError"
    assert c.retry_strategy.permanent_error_urls == [f"{B}/status/404", f"{B}/status/403"]


@check("День 5", "экспоненциальный backoff: паузы 0.1 → 0.2 → 0.4")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(retry_strategy=fast_retry())
    try:
        await c.fetch_url(f"{B}/flaky/kexp/3")
    finally:
        await c.close()
    g = gaps(port_of(B))
    expected = [0.1, 0.2, 0.4]
    assert len(g) == 3 and all(abs(a - e) < 0.07 for a, e in zip(g, expected)), (
        f"паузы {[round(x, 3) for x in g]}, ожидали ~{expected}")


@check("День 5", "429: пауза не меньше Retry-After")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(retry_strategy=fast_retry())
    try:
        html = await c.fetch_url(f"{B}/flaky429/k429/1")
    finally:
        await c.close()
    g = gaps(port_of(B))
    assert html and len(g) == 1, (html, g)
    assert g[0] >= 1.0 - EPS, f"пауза {g[0]:.2f} c, а сервер просил подождать 1 c"


@check("День 5", "500: только один повтор")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler(retry_strategy=fast_retry())
    try:
        r = await c.fetch_url(f"{B}/status/500")
    finally:
        await c.close()
    assert r is None and STATS.hits[(port_of(B), "/status/500")] == 2, dict(STATS.hits)
    assert c.error_details[f"{B}/status/500"]["attempts"] == 2


@check("День 5", "сетевая ошибка: не больше 2 повторов")
async def _(B, B2):
    url = f"http://127.0.0.1:{closed_port()}/"
    c = AsyncCrawler(retry_strategy=fast_retry(base_delay_by_type={NetworkError: 0.05}))
    try:
        r = await c.fetch_url(url)
    finally:
        await c.close()
    assert r is None
    assert c.error_details[url]["type"] == "NetworkError"
    assert c.error_details[url]["attempts"] == 3, c.error_details[url]


@check("День 5", "таймаут повторяется, и таймаут растёт с каждой попыткой")
async def _(B, B2):
    # Сервер думает 0.45 c, таймаут 0.3 c. Без роста все попытки упадут,
    # с ростом в 2 раза вторая попытка (0.6 c) успеет.
    url = f"{B}/slowhang/0.45"
    STATS.reset()
    c = AsyncCrawler(total_timeout=0.3, retry_strategy=fast_retry(max_retries=2, timeout_growth=1.0))
    try:
        r1 = await c.fetch_url(url)
    finally:
        await c.close()
    assert r1 is None and c.error_details[url]["type"] == "TransientError"
    assert STATS.hits[(port_of(B), "/slowhang/0.45")] == 3

    STATS.reset()
    c = AsyncCrawler(total_timeout=0.3, retry_strategy=fast_retry(max_retries=2, timeout_growth=2.0))
    try:
        r2 = await c.fetch_url(url)
    finally:
        await c.close()
    assert r2 and STATS.hits[(port_of(B), "/slowhang/0.45")] == 2, dict(STATS.hits)


@check("День 5", "execute_with_retry напрямую с fetch_once (пример из задания)")
async def _(B, B2):
    STATS.reset()
    c = AsyncCrawler()
    strategy = fast_retry(retry_on=[TransientError, NetworkError])
    try:
        html = await strategy.execute_with_retry(c.fetch_once, f"{B}/flaky/kdirect/1")
        try:
            await strategy.execute_with_retry(c.fetch_once, f"{B}/status/404")
            raised = None
        except PermanentError as e:
            raised = e
    finally:
        await c.close()
    assert html and STATS.flaky["kdirect"] == 2
    assert raised is not None and raised.status == 404 and raised.attempts == 1


@check("День 5", "circuit breaker: блокировка домена и восстановление")
async def _(B, B2):
    STATS.reset()
    breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=0.5)
    c = AsyncCrawler(circuit_breaker=breaker)
    try:
        for _ in range(3):
            await c.fetch_url(f"{B}/status/503")
        await c.fetch_url(f"{B}/ok")                 # автомат выбит — до сервера не дойдёт
        hits_while_open = sum(STATS.hits.values())
        await asyncio.sleep(0.6)
        probe = await c.fetch_url(f"{B}/ok?probe=1")  # пробный запрос
    finally:
        await c.close()
    assert hits_while_open == 3, f"пока автомат выбит, сервер получил {hits_while_open} запросов"
    assert c.error_details[f"{B}/ok"]["type"] == "CircuitOpenError"
    assert probe and breaker.state(f"127.0.0.1:{port_of(B)}") == "closed"


@check("День 5", "статистика ошибок")
async def _(B, B2):
    c = AsyncCrawler(retry_strategy=fast_retry())
    try:
        await c.fetch_urls([f"{B}/flaky/kstat/1", f"{B}/status/404", f"{B}/status/500", f"{B}/ok"])
    finally:
        await c.close()
    st = c.get_error_stats()
    r = st["retries"]
    assert st["final_errors_by_type"] == {"PermanentError": 1, "TransientError": 1}, st
    assert st["permanent_error_urls"] == [f"{B}/status/404"]
    assert r["errors_by_type"] == {"TransientError": 3, "PermanentError": 1}, r
    assert r["total_retries"] == 2 and r["successful_retries"] == 1, r
    assert r["failed_after_retries"] == 1 and abs(r["avg_retry_delay"] - 0.1) < 1e-9, r


# ---------- День 6 ----------

TRICKY = 'Цена: 12,990 ₽; "скидка" 10%\nвторая строка; запятая, кавычка " и табуляция\t'

RECORDS = [
    {"url": "https://s.test/1", "title": TRICKY, "text": "текст страницы",
     "links": ["https://s.test/2", "https://other.test/"],
     "metadata": {"description": "описание, с запятой"}, "status_code": 200,
     "content_type": "text/html; charset=utf-8"},
    {"url": "https://s.test/2", "title": "Вторая", "text": "", "links": [], "metadata": {},
     "status_code": 200, "content_type": "text/html"},
    {"url": "https://s.test/3", "title": "Третья", "text": "x" * 5000, "links": ["https://s.test/1"],
     "metadata": {"keywords": "a, b"}, "status_code": 301, "content_type": None},
]


def tmpdir() -> Path:
    return Path(tempfile.mkdtemp(prefix="crawler_check_"))


def same_records(back: list[dict]) -> None:
    """Прочитанное совпадает с записанным по всем значимым полям."""
    assert len(back) == len(RECORDS), f"записей {len(back)}, ожидали {len(RECORDS)}"
    for orig, got in zip(RECORDS, back):
        for key in ("url", "title", "text", "links", "metadata", "status_code"):
            assert got[key] == orig[key], f"{orig['url']}: поле {key} не совпало: {got[key]!r}"
        datetime.fromisoformat(got["crawled_at"])     # дата читается обратно


async def save_all(storage, records=RECORDS) -> None:
    for r in records:
        await storage.save(r)
    await storage.close()


@check("День 6", "JSON Lines: запись и чтение без потерь")
async def _(B, B2):
    path = tmpdir() / "r.jsonl"
    await save_all(JSONStorage(path, batch_size=2))
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3, f"строк в файле {len(lines)}, ожидали 3 — по одной на запись"
    same_records(await JSONStorage(path).read_all())


@check("День 6", "JSON pretty: валидный массив с отступами, даже пустой")
async def _(B, B2):
    d = tmpdir()
    await save_all(JSONStorage(d / "r.json", pretty=True, batch_size=2))
    text = (d / "r.json").read_text(encoding="utf-8")
    assert text.startswith("[") and "\n  {" in text, text[:80]
    same_records(json.loads(text))
    await JSONStorage(d / "empty.json", pretty=True).close()
    assert json.loads((d / "empty.json").read_text(encoding="utf-8")) == []


@check("День 6", "CSV: заголовки, спецсимволы, кодировки")
async def _(B, B2):
    d = tmpdir()
    await save_all(CSVStorage(d / "r.csv", batch_size=2))
    header = (d / "r.csv").read_text(encoding="utf-8").split("\n", 1)[0]
    assert header.split(",") == list(STANDARD_FIELDS), header
    same_records(await CSVStorage(d / "r.csv").read_all())

    await save_all(CSVStorage(d / "bom.csv", encoding="utf-8-sig"))
    assert (d / "bom.csv").read_bytes()[:3] == b"\xef\xbb\xbf", "нет метки BOM для Excel"

    cyr = [{"url": "https://s.test/ru", "title": "Кофемашина, «Делонги»", "text": "привет",
            "links": [], "metadata": {}, "status_code": 200}]
    await save_all(CSVStorage(d / "cp.csv", encoding="cp1251"), cyr)
    assert "Кофемашина".encode("cp1251") in (d / "cp.csv").read_bytes()
    back = await CSVStorage(d / "cp.csv", encoding="cp1251").read_all()
    assert back[0]["title"] == "Кофемашина, «Делонги»", back[0]["title"]


@check("День 6", "SQLite: таблица, индексы, повтор URL не дублирует строку")
async def _(B, B2):
    path = tmpdir() / "r.db"
    await save_all(SQLiteStorage(path, batch_size=2))
    same_records(await SQLiteStorage(path).read_all())

    con = sqlite3.connect(path)
    indexes = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='index'")}
    assert {"idx_pages_crawled_at", "idx_pages_status"} <= indexes, indexes
    con.close()

    st = SQLiteStorage(path)
    await save_all(st, [dict(RECORDS[0], title="Новый заголовок")])
    rows = await SQLiteStorage(path).query("SELECT title FROM pages WHERE url = ?", (RECORDS[0]["url"],))
    assert rows == [{"title": "Новый заголовок"}], rows


@check("День 6", "пакетная запись: 25 записей, batch_size=10 → 3 пачки")
async def _(B, B2):
    st = SQLiteStorage(tmpdir() / "b.db", batch_size=10)
    for i in range(25):
        await st.save({"url": f"https://s.test/{i}", "title": str(i)})
    assert st.batches == 2 and st.get_stats()["buffered"] == 5, st.get_stats()
    await st.close()
    assert st.batches == 3 and st.saved == 25, st.get_stats()
    rows = await SQLiteStorage(st.path).query("SELECT COUNT(*) AS n FROM pages")
    assert rows == [{"n": 25}], rows


@check("День 6", "ошибка записи: повторы, лог, работа продолжается")
async def _(B, B2):
    d = tmpdir()
    (d / "taken").mkdir()                 # на месте файла — папка: записать нельзя
    st = JSONStorage(d / "taken", write_retries=2, retry_delay=0.01)
    await save_all(st)                    # не должно бросить исключение
    stats = st.get_stats()
    assert stats["saved"] == 0 and stats["failed"] == 3, stats
    assert stats["write_errors"] == 3, f"попыток записи {stats['write_errors']}, ожидали 1 + 2 повтора"


@check("День 6", "временная ошибка записи: успех после повтора")
async def _(B, B2):
    class FlakyJSON(JSONStorage):
        calls = 0

        async def _write_batch(self, records):
            FlakyJSON.calls += 1
            if FlakyJSON.calls == 1:
                raise OSError("диск занят")
            await super()._write_batch(records)

    path = tmpdir() / "flaky.jsonl"
    st = FlakyJSON(path, retry_delay=0.01)
    await save_all(st)
    assert st.saved == 3 and st.failed == 0 and st.write_errors == 1, st.get_stats()
    same_records(await JSONStorage(path).read_all())


@check("День 6", "fetch_and_parse отдаёт код ответа и тип содержимого")
async def _(B, B2):
    c = AsyncCrawler()
    try:
        ok = await c.fetch_and_parse(f"{B}/parse")
        bad = await c.fetch_and_parse(f"{B}/status/404")
    finally:
        await c.close()
    assert ok["status_code"] == 200 and ok["content_type"].startswith("text/html"), ok
    assert bad["status_code"] == 404 and set(ok) == set(bad)


@check("День 6", "обход сохраняет каждую страницу в базу")
async def _(B, B2):
    path = tmpdir() / "crawl.db"
    c = AsyncCrawler(max_depth=5, storage=SQLiteStorage(path, batch_size=4))
    try:
        res = await c.crawl([f"{B}/site/"], same_domain_only=True)
    finally:
        await c.close()                   # close() обязан дописать буфер
    rows = await SQLiteStorage(path).read_all()
    assert {r["url"] for r in rows} == set(res), (len(rows), len(res))
    assert len(rows) == 9, len(rows)
    assert all(r["status_code"] == 200 and r["content_type"].startswith("text/html") for r in rows)
    assert all(datetime.fromisoformat(r["crawled_at"]) for r in rows)


@check("День 6", "сломанное хранилище не останавливает обход")
async def _(B, B2):
    d = tmpdir()
    (d / "taken").mkdir()
    st = JSONStorage(d / "taken", batch_size=3, write_retries=1, retry_delay=0.01)
    c = AsyncCrawler(max_depth=5, storage=st)
    try:
        res = await c.crawl([f"{B}/site/"], same_domain_only=True)
    finally:
        await c.close()
    assert len(res) == 9, f"обработано {len(res)} страниц, ожидали 9"
    assert st.failed == 9 and st.saved == 0, st.get_stats()


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