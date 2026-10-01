"""
День 7, пункт 10 — тесты производительности.

    python -m demos.perf_day7

Всё на локальном сервере, интернет не нужен. Чтобы было похоже на
настоящий сайт, сервер отвечает с задержкой 50 мс — примерно столько
занимает дорога до сервера и обратно в реальной сети. Без задержки
на localhost ответ приходит мгновенно, и сравнение с синхронным
кодом потеряло бы смысл: ждать было бы нечего.

1. Синхронный краулер против асинхронного на одних и тех же страницах.
2. Масштабируемость: 100, 500 и 1000 страниц — время, скорость, память.
3. Узкое место: извлечение текста при разборе HTML — как было и как стало.

Занимает около минуты.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import tracemalloc
import urllib.request
from collections import deque

from bs4 import BeautifulSoup

from crawler import AsyncCrawler, HTMLParser
from crawler.html_parser import NON_CONTENT_TAGS
from tests.check_all import Handler, TestServer, page

LATENCY = 0.05          # «сетевая» задержка ответа, секунды


class SlowHandler(Handler):
    """Дерево страниц /lat/i → /lat/5i+1 … /lat/5i+5, каждая с задержкой."""

    def _route(self, path: str) -> tuple:
        if path.startswith("/lat/"):
            time.sleep(LATENCY)
            i = int(path.rsplit("/", 1)[1])
            links = [f"/lat/{5 * i + k}" for k in range(1, 6)]
            text = f"<p>Страница {i}. " + "Немного обычного текста. " * 40 + "</p>"
            return 200, page(f"Страница {i}", *links, extra=text)
        return super()._route(path)


def start_slow_server() -> tuple[TestServer, str]:
    server = TestServer(("127.0.0.1", 0), SlowHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


def header(title: str) -> None:
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}")


def sync_crawl(start: str, max_pages: int) -> int:
    """
    Обычный синхронный краулер: urllib + BeautifulSoup, страницы по одной.
    Пока ждём ответ, программа просто стоит.
    """
    parser = HTMLParser()
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    queue, seen, done = deque([start]), {start}, 0
    while queue and done < max_pages:
        url = queue.popleft()
        with opener.open(url, timeout=10) as r:
            html = r.read().decode("utf-8")
        done += 1
        soup = BeautifulSoup(html, parser.parser)
        for link in parser.extract_links(soup, url):
            if link not in seen:
                seen.add(link)
                queue.append(link)
    return done


async def async_crawl(start: str, max_pages: int, concurrency: int) -> int:
    c = AsyncCrawler(max_concurrent=concurrency, max_depth=20, progress_interval=3600)
    try:
        res = await c.crawl([start], max_pages=max_pages, same_domain_only=True)
    finally:
        await c.close()
    return len(res)


async def compare_sync_async(base: str) -> None:
    header(f"1. Синхронный против асинхронного: 100 страниц, задержка ответа {LATENCY * 1000:.0f} мс")
    n = 100

    t = time.perf_counter()
    done = await asyncio.to_thread(sync_crawl, f"{base}/lat/0", n)
    sync_time = time.perf_counter() - t
    print(f"\n  синхронно (по одной)   : {done} стр. за {sync_time:5.2f} c  "
          f"→ {done / sync_time:6.1f} стр/с")

    for conc in (5, 20):
        t = time.perf_counter()
        done = await async_crawl(f"{base}/lat/0", n, conc)
        dt = time.perf_counter() - t
        print(f"  асинхронно, {conc:>2} разом  : {done} стр. за {dt:5.2f} c  "
              f"→ {done / dt:6.1f} стр/с   (в {sync_time / dt:.1f} раза быстрее)")

    print(f"\n    Синхронный ждёт каждый ответ: 100 × {LATENCY * 1000:.0f} мс ≈ {100 * LATENCY:.0f} c только")
    print("    на ожидание. Асинхронный ждёт десятки ответов одновременно.")


async def scalability(base: str) -> None:
    """2. Масштабируемость."""
    header("2. Масштабируемость: 100 / 500 / 1000 страниц, 20 запросов разом")
    print(f"\n  {'страниц':>8} {'время':>8} {'стр/с':>8} {'пик памяти':>12} {'на страницу':>12}")
    for n in (100, 500, 1000):
        t = time.perf_counter()
        done = await async_crawl(f"{base}/lat/0", n, 20)
        dt = time.perf_counter() - t

        # Память меряем отдельным прогоном: tracemalloc сам замедляет программу
        tracemalloc.start()
        await async_crawl(f"{base}/lat/0", n, 20)
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        print(f"  {done:>8} {dt:>7.2f}c {done / dt:>8.1f} {peak / 2**20:>10.1f} МБ "
              f"{peak / done / 1024:>9.1f} КБ")
    print("\n    Скорость почти не падает с ростом объёма — время растёт линейно.")
    print("    Память тоже растёт линейно: crawl() держит разобранные страницы")
    print("    в processed_urls. Для миллионов страниц их стоит сразу сохранять")
    print("    в хранилище (день 6), а не копить в словаре.")


def old_clean_text(node) -> str:
    """Как было до дня 7: копия дерева через str() и повторный разбор."""
    copy = BeautifulSoup(str(node), "html.parser")
    for tag in copy(NON_CONTENT_TAGS):
        tag.decompose()
    return copy.get_text(separator=" ", strip=True)


def bottleneck() -> None:
    header("3. Узкое место: извлечение текста из HTML")
    blocks = "".join(
        f"<div class='item'><h3>Товар {i}</h3><p>Описание товара {i}. "
        + "Слова для объёма. " * 15 + f"</p><script>track({i})</script>"
        f"<a href='/p/{i}'>подробнее</a></div>"
        for i in range(300)
    )
    html = f"<html><head><title>Каталог</title><style>.a{{}}</style></head><body>{blocks}</body></html>"
    parser = HTMLParser()
    soup = BeautifulSoup(html, parser.parser)
    print(f"\n  Страница {len(html) / 1024:.0f} КБ, 300 карточек товаров.")

    def best_of(fn, repeat: int = 5) -> float:
        times = []
        for _ in range(repeat):
            t = time.perf_counter()
            fn()
            times.append(time.perf_counter() - t)
        return min(times) * 1000

    old = best_of(lambda: old_clean_text(soup))
    new = best_of(lambda: parser._clean_text(soup))
    same = old_clean_text(soup) == parser._clean_text(soup)
    print(f"\n  извлечение текста, было : {old:7.1f} мс")
    print(f"  извлечение текста, стало: {new:7.1f} мс   (в {old / new:.0f} раз быстрее)")
    print(f"  результат совпадает     : {'да' if same else 'НЕТ'}")

    total = best_of(lambda: asyncio.run(parser.parse_html(html, "https://shop.test/")), 3)
    print(f"  весь parse_html сейчас  : {total:7.1f} мс")
    print("\n    Было: на каждый вызов текст страницы превращался обратно в строку")
    print("    и разбирался ЗАНОВО — второй полный разбор HTML. Стало: один проход")
    print("    по уже готовому дереву, служебные теги просто пропускаются.")
    print("    Разбор — работа процессора (CPU-bound): пока он идёт, event loop")
    print("    стоит и не обслуживает сеть. Поэтому ускорение разбора ускоряет весь обход.")


async def network_tests() -> None:
    server, base = start_slow_server()
    try:
        await compare_sync_async(base)
        await scalability(base)
    finally:
        server.shutdown()


def main() -> None:
    logging.basicConfig(level=logging.CRITICAL)
    asyncio.run(network_tests())
    bottleneck()            # внутри свой asyncio.run, поэтому вне network_tests
    print("\nГотово.\n")


if __name__ == "__main__":
    main()