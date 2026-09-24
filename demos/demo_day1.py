"""
День 1 — демонстрация (пункты 6 и 7 задания).

    pip install -r requirements.txt
    python demo_day1.py

Пункт 6 — демонстрация:
    загружаем 8 URL параллельно, выводим статус каждого,
    измеряем общее время.

Пункт 7 — тестирование:
    валидные URL, несуществующие URL, таймауты,
    сравнение последовательной и параллельной загрузки.
"""

import asyncio
import logging
import time

from crawler import AsyncCrawler

# Если httpbin.org недоступен (с ним это бывает), замени на
# https://postman-echo.com — там тоже есть /delay/{n}
HTTPBIN = "https://httpbin.org"


def setup_logging() -> None:
    """
    Пункт 5 задания. Настройку логирования делает ПРИЛОЖЕНИЕ,
    а не библиотека: класс только пишет в logging.getLogger(__name__).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def header(title: str) -> None:
    print(f"\n{'=' * 62}\n  {title}\n{'=' * 62}")


# ============================================================
# Пункт 6. Демонстрация: параллельная загрузка
# ============================================================

async def demo_parallel_download() -> None:
    header("Пункт 6. Параллельная загрузка 8 URL")

    urls = [
        "https://example.com",
        "https://www.python.org",
        "https://docs.aiohttp.org/en/stable/",
        f"{HTTPBIN}/get",
        f"{HTTPBIN}/html",
        f"{HTTPBIN}/user-agent",
        f"{HTTPBIN}/delay/1",
        f"{HTTPBIN}/delay/2",
    ]

    crawler = AsyncCrawler(max_concurrent=5)
    try:
        started = time.perf_counter()
        pages = await crawler.fetch_urls(urls)
        elapsed = time.perf_counter() - started
    finally:
        await crawler.close()

    # Статус каждого запроса
    print("\n  Статус по каждому URL:")
    for url in urls:
        if url in pages:
            print(f"    OK     {url:<45} {len(pages[url]):>8} символов")
        else:
            print(f"    ОШИБКА {url:<45} {crawler.errors.get(url, '')}")

    print(f"\n  Успешно: {crawler.successful}, с ошибкой: {crawler.failed}")
    print(f"  Общее время: {elapsed:.2f} c")


# ============================================================
# Пункт 7. Сравнение последовательной и параллельной загрузки
# ============================================================

async def test_sequential_vs_parallel() -> None:
    header("Пункт 7. Последовательно против параллельно")

    # Эндпоинты с ИЗВЕСТНОЙ задержкой — так эффект виден чисто,
    # без шума от разной скорости реальных сайтов.
    delays = [1, 1, 1, 2, 2]
    urls = [f"{HTTPBIN}/delay/{d}" for d in delays]

    print(f"\n  {len(urls)} запросов с задержками {delays} c")
    print(f"  Ожидаем: последовательно ~{sum(delays)} c, "
          f"параллельно ~{max(delays)} c\n")

    # Последовательно: await на каждом URL по очереди
    crawler = AsyncCrawler(max_concurrent=10)
    try:
        started = time.perf_counter()
        for url in urls:
            await crawler.fetch_url(url)
        sequential = time.perf_counter() - started
    finally:
        await crawler.close()

    # Параллельно: тот же список через fetch_urls
    crawler = AsyncCrawler(max_concurrent=10)
    try:
        started = time.perf_counter()
        await crawler.fetch_urls(urls)
        parallel = time.perf_counter() - started
    finally:
        await crawler.close()

    print(f"\n  Последовательно : {sequential:6.2f} c")
    print(f"  Параллельно     : {parallel:6.2f} c")
    print(f"  Ускорение       : {sequential / parallel:6.2f}x")


# ============================================================
# Пункт 7. Несуществующие и битые URL
# ============================================================

async def test_invalid_urls() -> None:
    header("Пункт 7. Обработка несуществующих URL")

    urls = [
        "https://example.com",                      # валидный
        f"{HTTPBIN}/status/404",                    # страницы нет
        f"{HTTPBIN}/status/500",                    # ошибка сервера
        "https://такого-домена-не-существует-999.com",  # DNS не резолвится
    ]

    crawler = AsyncCrawler(max_concurrent=5)
    try:
        pages = await crawler.fetch_urls(urls)
    finally:
        await crawler.close()

    print(f"\n  Успешно: {crawler.successful}, с ошибкой: {crawler.failed}")
    print("  Перехваченные ошибки:")
    for url, reason in crawler.errors.items():
        print(f"    {url:<48} {reason}")
    print("\n  Программа не упала — ни одно исключение не вышло наружу.")


# ============================================================
# Пункт 7. Таймауты
# ============================================================

async def test_timeout() -> None:
    header("Пункт 7. Таймауты")

    urls = [f"{HTTPBIN}/delay/10", f"{HTTPBIN}/delay/1"]
    print("\n  total_timeout=3 c, первый URL отвечает через 10 c\n")

    crawler = AsyncCrawler(max_concurrent=5, total_timeout=3.0)
    try:
        started = time.perf_counter()
        pages = await crawler.fetch_urls(urls)
        elapsed = time.perf_counter() - started
    finally:
        await crawler.close()

    print(f"\n  Успешно: {crawler.successful}, с ошибкой: {crawler.failed}")
    for url, reason in crawler.errors.items():
        print(f"    {url} → {reason}")
    print(f"  Уложились в {elapsed:.2f} c вместо 10 — таймаут сработал.")


async def main() -> None:
    setup_logging()
    await demo_parallel_download()
    await test_sequential_vs_parallel()
    await test_invalid_urls()
    await test_timeout()
    print("\nГотово.\n")


if __name__ == "__main__":
    asyncio.run(main())
