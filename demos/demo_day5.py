"""
День 5 — демонстрация (пункты 10 и 11 задания).

    python -m demos.demo_day5

Части 1-6 — на локальном тестовом сервере (тот же, что в
tests/check_all.py). У него есть «капризные» адреса: отвечают ошибкой
первые N раз, а потом нормально. Сервер сам считает, сколько попыток
к нему пришло.

Часть 7 — настоящие сайты с разными ошибками и отчёт в
output/day5_errors.json.
"""

import asyncio
import json
import logging
import time
from pathlib import Path

from crawler import AsyncCrawler, CircuitBreaker, RetryStrategy
from crawler.errors import classify_status
from tests.check_all import STATS, Handler, closed_port, start_server


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)-15s | %(message)s",
        datefmt="%H:%M:%S",
    )
    # Попытки и итоги видны (пункт 8), построчные «Начинаю загрузку» — нет
    logging.getLogger("crawler.async_crawler").setLevel(logging.WARNING)
    logging.getLogger("crawler.html_parser").setLevel(logging.WARNING)
    logging.getLogger("crawler.progress").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def header(title: str) -> None:
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}")


def port_of(base: str) -> int:
    return int(base.rsplit(":", 1)[1])


def server_gaps(base: str) -> list[float]:
    times = [t for t, _ in STATS.starts[port_of(base)]]
    return [round(b - a, 2) for a, b in zip(times, times[1:])]


def quick_retry(**kw) -> RetryStrategy:
    """Короткие паузы, чтобы демо шло быстро. В бою паузы больше."""
    params = dict(max_retries=3, base_delay=0.2, backoff_factor=2.0,
                  base_delay_by_type={}, timeout_growth=2.0)
    params.update(kw)
    return RetryStrategy(**params)


def demo_classification() -> None:
    """1. Классификация."""
    header("1. Классификация ошибок по HTTP-коду")
    strategy = RetryStrategy()
    print(f"\n  {'Код':<6}{'Тип':<20}{'Повторять?':<12}Сколько раз")
    for status in (429, 503, 502, 500, 404, 403, 401):
        err = classify_status("u", status, "")
        retry = strategy.should_retry(err)
        times = strategy.max_retries_for(err) if retry else 0
        print(f"  {status:<6}{type(err).__name__:<20}{'да' if retry else 'нет':<12}{times}")
    print("\n    Временные и сетевые — повторяем. Постоянные — сразу сдаёмся.")
    print("    500 — повторяем один раз: сервер сломан, но вдруг это разовый сбой.")


async def demo_retry_503(B: str) -> None:
    """2. Повторы при 503."""
    header("2. Сервер дважды отвечает 503, потом нормально")
    STATS.reset()
    crawler = AsyncCrawler(retry_strategy=quick_retry())
    try:
        html = await crawler.fetch_url(f"{B}/flaky/demo503/2")
    finally:
        await crawler.close()
    print(f"\n  Результат          : {'страница получена' if html else 'неудача'}")
    print(f"  Попыток на сервере : {STATS.flaky['demo503']}")
    print(f"  Паузы между ними   : {server_gaps(B)}  ← 0.2, потом 0.4: растут вдвое")


async def demo_no_retry(B: str) -> None:
    """3. Постоянные ошибки не повторяются."""
    header("3. 404 и 403 — не повторяем")
    STATS.reset()
    crawler = AsyncCrawler(retry_strategy=quick_retry())
    try:
        await crawler.fetch_url(f"{B}/status/404")
        await crawler.fetch_url(f"{B}/status/403")
    finally:
        await crawler.close()
    hits = {path: n for (_, path), n in STATS.hits.items()}
    print(f"\n  Запросы на сервере: {hits}")
    print("    По одному разу: страницы нет или нас не пускают — повтор не поможет.")


async def demo_429(B: str) -> None:
    """4. 429 и Retry-After."""
    header("4. 429 Too Many Requests с Retry-After: 1")
    STATS.reset()
    crawler = AsyncCrawler(retry_strategy=quick_retry())
    try:
        html = await crawler.fetch_url(f"{B}/flaky429/demo429/1")
    finally:
        await crawler.close()
    print(f"\n  Результат : {'страница получена' if html else 'неудача'}")
    print(f"  Пауза     : {server_gaps(B)} c")
    print("    Наша пауза была бы 0.2 c, но сервер попросил 1 c — ждём, сколько просят.")


async def demo_timeouts(B: str) -> None:
    """5. Таймауты."""
    header("5. Таймаут: сервер думает 0.45 c, ждём 0.3 c")
    url = f"{B}/slowhang/0.45"
    for growth in (1.0, 2.0):
        STATS.reset()
        crawler = AsyncCrawler(total_timeout=0.3,
                               retry_strategy=quick_retry(max_retries=2, timeout_growth=growth))
        try:
            html = await crawler.fetch_url(url)
        finally:
            await crawler.close()
        attempts = STATS.hits[(port_of(B), "/slowhang/0.45")]
        print(f"\n  timeout_growth={growth}: {'УСПЕХ' if html else 'неудача'}, попыток {attempts}")
    print("\n    Без роста все попытки упираются в тот же таймаут.")
    print("    С ростом вторая попытка ждёт 0.6 c и успевает.")


async def demo_circuit_breaker(B: str) -> None:
    """6. Circuit breaker."""
    header("6. Circuit breaker: 3 ошибки подряд — домен на паузу")
    STATS.reset()
    breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=1.0)
    crawler = AsyncCrawler(circuit_breaker=breaker)
    domain = f"127.0.0.1:{port_of(B)}"
    try:
        for i in range(5):
            await crawler.fetch_url(f"{B}/status/503?n={i}")
            print(f"  запрос {i + 1}: автомат {breaker.state(domain):<9} "
                  f"на сервер дошло {sum(STATS.hits.values())}")
        print("\n  ...ждём 1 c...")
        await asyncio.sleep(1.1)
        html = await crawler.fetch_url(f"{B}/ok")
        print(f"  пробный запрос: {'успех' if html else 'неудача'}, автомат {breaker.state(domain)}")
    finally:
        await crawler.close()
    print(f"\n  Отклонено без запроса: {breaker.rejected}")
    print("    Запросы 4 и 5 до сервера не дошли: автомат «выбит».")


async def demo_live_report() -> None:
    """7. Настоящие сайты и отчёт."""
    header("7. Настоящие сайты с разными ошибками")

    urls = [
        "https://example.com",                          # всё хорошо
        "https://httpbin.org/status/503",               # всегда 503 — повторим и сдадимся
        "https://httpbin.org/status/404",               # постоянная — не повторяем
        "https://httpbin.org/status/500",               # один повтор
        "https://httpbin.org/delay/3",                  # медленно — таймаут растёт
        "https://no-such-host.test/",                   # DNS — сетевая ошибка
    ]
    strategy = RetryStrategy(max_retries=3, backoff_factor=2.0, base_delay=0.5,
                             base_delay_by_type={}, timeout_growth=2.0)
    crawler = AsyncCrawler(total_timeout=2.0, retry_strategy=strategy,
                           user_agent="MyBot/1.0 (educational project)")
    started = time.perf_counter()
    try:
        pages = await crawler.fetch_urls(urls)
    finally:
        await crawler.close()
    elapsed = time.perf_counter() - started

    print(f"\n  {'URL':<38}{'Итог':<16}{'Тип':<16}Попыток")
    for url in urls:
        if url in pages:
            print(f"  {url:<38}{'OK':<16}{'':<16}")
        else:
            d = crawler.error_details.get(url, {})
            print(f"  {url:<38}{'ошибка':<16}{d.get('type', ''):<16}{d.get('attempts', '')}")

    stats = crawler.get_error_stats()
    r = stats["retries"]
    print(f"\n  Время              : {elapsed:.1f} c")
    print(f"  Ошибок по типам    : {r['errors_by_type']}  (каждая неудачная попытка)")
    print(f"  Итоговые неудачи   : {stats['final_errors_by_type']}")
    print(f"  Повторов всего     : {r['total_retries']}, удачных: {r['successful_retries']}")
    print(f"  Средняя пауза      : {r['avg_retry_delay']:.2f} c")
    print(f"  Постоянные ошибки  : {r['permanent_error_urls']}")

    # Пункт 10: отчёт об ошибках
    out = Path("output") / "day5_errors.json"
    out.parent.mkdir(exist_ok=True)
    report = {"elapsed_sec": round(elapsed, 2), "stats": stats, "errors": crawler.error_details}
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n  Отчёт сохранён в {out}")


async def main() -> None:
    setup_logging()
    demo_classification()
    server1, B = start_server()
    server2, B2 = start_server()
    Handler.external = f"{B2}/site/x"
    try:
        await demo_retry_503(B)
        await demo_no_retry(B)
        await demo_429(B)
        await demo_timeouts(B)
        await demo_circuit_breaker(B)
    finally:
        server1.shutdown()
        server2.shutdown()
    await demo_live_report()
    print("\nГотово.\n")


if __name__ == "__main__":
    asyncio.run(main())