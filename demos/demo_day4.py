"""
День 4 — демонстрация (пункты 8 и 9 задания).

    python -m demos.demo_day4

Части 1-5 работают на локальном тестовом сервере (тот же, что
в tests/check_all.py): интернет не нужен, а сервер сам записывает время
каждого запроса. Поэтому паузы видно глазами сайта, а не краулера.

Часть 6 — вежливый обход настоящего сайта books.toscrape.com
и проверка robots.txt настоящей Википедии.
"""

import asyncio
import logging
import time

from crawler import AsyncCrawler, RobotsParser
from tests.check_all import STATS, Handler, start_server


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("crawler.async_crawler").setLevel(logging.ERROR)
    logging.getLogger("crawler.html_parser").setLevel(logging.WARNING)
    logging.getLogger("crawler.robots_parser").setLevel(logging.WARNING)
    logging.getLogger("crawler.progress").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def header(title: str) -> None:
    print(f"\n{'=' * 64}\n  {title}\n{'=' * 64}")


def port_of(base: str) -> int:
    return int(base.rsplit(":", 1)[1])


def server_gaps(base: str) -> list[float]:
    """Паузы между запросами, как их видел сервер (без robots.txt)."""
    times = [t for t, path in STATS.starts[port_of(base)] if path != "/robots.txt"]
    return [round(b - a, 2) for a, b in zip(times, times[1:])]


# ============================================================
# 1. Лимит скорости на одном домене
# ============================================================

async def demo_one_domain(B: str) -> None:
    header("1. Лимит скорости: один домен, 5 запросов в секунду")

    STATS.reset()
    crawler = AsyncCrawler(max_concurrent=10, requests_per_second=5)
    started = time.perf_counter()
    try:
        await crawler.fetch_urls([f"{B}/ok?n={i}" for i in range(6)])
    finally:
        await crawler.close()

    print("\n  6 запросов отправлены РАЗОМ, семафор пускает все 10.")
    print(f"  Паузы между запросами на сервере: {server_gaps(B)}")
    print(f"  Всего: {time.perf_counter() - started:.2f} c (без лимита было бы ~0.0)")
    print("\n    Семафор не мешает, но лимитер выпускает запросы")
    print("    не чаще одного в 0.2 c.")


# ============================================================
# 2. Разные домены — независимые лимиты
# ============================================================

async def demo_two_domains(B: str, B2: str) -> None:
    header("2. Лимит скорости: два домена по 2 запроса в секунду")

    for per_domain in (True, False):
        STATS.reset()
        crawler = AsyncCrawler(max_concurrent=10, requests_per_second=2,
                               rate_per_domain=per_domain)
        started = time.perf_counter()
        try:
            await crawler.fetch_urls(
                [f"{base}/ok?n={i}" for i in range(3) for base in (B, B2)]
            )
        finally:
            await crawler.close()
        label = "у каждого свой лимит" if per_domain else "один общий лимит    "
        print(f"\n  rate_per_domain={per_domain!s:<5} ({label}): "
              f"{time.perf_counter() - started:.2f} c")
        print(f"    сайт 1: паузы {server_gaps(B)}")
        print(f"    сайт 2: паузы {server_gaps(B2)}")

    print("\n    Раздельные лимиты: сайты качаются параллельно, каждому")
    print("    по 2 запроса в секунду. Общий лимит: 6 запросов в очередь")
    print("    на всех — дольше.")


# ============================================================
# 3. Разбор robots.txt
# ============================================================

ROBOTS_EXAMPLE = """
User-agent: *
Disallow: /admin
Allow: /admin/public
Disallow: /*.pdf$
Crawl-delay: 2

User-agent: MyBot
Disallow: /mybot-only

User-agent: BadBot
Disallow: /
"""


async def demo_robots_parsing() -> None:
    header("3. Разбор robots.txt")
    print(ROBOTS_EXAMPLE.replace("\n", "\n    "))

    robots = RobotsParser()
    robots.parse(ROBOTS_EXAMPLE, "https://shop.test/robots.txt")

    cases = [
        ("/catalog", "*"),
        ("/admin/settings", "*"),
        ("/admin/public/help", "*"),
        ("/docs/price.pdf", "*"),
        ("/admin/settings", "MyBot/1.0"),
        ("/mybot-only", "MyBot/1.0"),
        ("/catalog", "BadBot/2.0"),
    ]
    print(f"  {'Робот':<12} {'Адрес':<22} Можно?")
    for path, ua in cases:
        allowed = robots.can_fetch(f"https://shop.test{path}", ua)
        print(f"  {ua:<12} {path:<22} {'да' if allowed else 'НЕТ'}")

    print(f"\n  Crawl-delay для *     : {robots.get_crawl_delay('*')} c")
    print(f"  Crawl-delay для MyBot : {robots.get_crawl_delay('MyBot/1.0')} c")
    print("\n    /admin/public разрешён: правило Allow длиннее, чем Disallow /admin.")
    print("    У MyBot своя группа — общие запреты на него не действуют.")


# ============================================================
# 4. Блокировка запрещённых адресов и Crawl-delay
# ============================================================

async def demo_robots_crawl(B: str) -> None:
    header("4. Обход с соблюдением robots.txt")

    print("\n  robots.txt тестового сайта:")
    print("    User-agent: *")
    print("    Disallow: /site/private")
    print("    Crawl-delay: 0.3\n")

    STATS.reset()
    crawler = AsyncCrawler(max_concurrent=10, max_depth=2, respect_robots=True,
                           user_agent="MyBot/1.0")
    try:
        results = await crawler.crawl([f"{B}/site/"], same_domain_only=True)
    finally:
        await crawler.close()

    hits = sorted(path for (port, path) in STATS.hits if port == port_of(B))
    print(f"  Обработано страниц : {len(results)}")
    print(f"  Заблокировано      : {sorted(u.replace(B, '') for u in crawler.blocked_urls)}")
    print(f"  Сервер получил     : {hits}")
    print(f"  Паузы на сервере   : {server_gaps(B)}")

    stats = crawler.get_rate_stats()
    print(f"\n  Средняя пауза      : {stats['avg_interval']:.2f} c")
    print(f"  Среднее ожидание   : {stats['avg_wait']:.2f} c на запрос")
    print(f"  Запрещено robots   : {stats['blocked_by_robots']}")

    print("\n    /site/private/login до сервера не дошёл вообще.")
    print("    robots.txt скачан один раз. Паузы не меньше 0.3 c —")
    print("    это Crawl-delay, хотя сами мы лимит скорости не задавали.")


# ============================================================
# 5. Задержки: min_delay, jitter, backoff
# ============================================================

async def demo_delays(B: str) -> None:
    header("5. Задержки: min_delay, jitter, backoff")

    STATS.reset()
    crawler = AsyncCrawler(max_concurrent=10, min_delay=0.2, jitter=0.2)
    try:
        await crawler.fetch_urls([f"{B}/ok?n={i}" for i in range(7)])
    finally:
        await crawler.close()
    print(f"\n  min_delay=0.2, jitter=0.2: паузы {server_gaps(B)}")
    print("    Все от 0.2 до 0.4 c и разные — не метроном.")

    STATS.reset()
    crawler = AsyncCrawler(max_concurrent=1, error_backoff=0.2)
    try:
        for path in ("/status/500", "/status/500", "/status/500", "/ok", "/ok?n=2"):
            await crawler.fetch_url(f"{B}{path}")
    finally:
        await crawler.close()
    print("\n  error_backoff=0.2, запросы: 500, 500, 500, OK, OK")
    print(f"  Паузы на сервере: {server_gaps(B)}")
    print("    Каждая ошибка подряд удваивает паузу: 0.2 → 0.4 → 0.8.")
    print("    Первый успешный ответ снимает замедление.")


# ============================================================
# 6. Настоящие сайты
# ============================================================

async def demo_live() -> None:
    header("6. Вежливый обход books.toscrape.com")

    print("\n  2 запроса в секунду, минимум 0.5 c между запросами,")
    print("  robots.txt соблюдается, не больше 10 страниц.\n")

    crawler = AsyncCrawler(
        max_concurrent=5,
        requests_per_second=2.0,
        respect_robots=True,
        min_delay=0.5,
        user_agent="MyBot/1.0 (educational project)",
        max_depth=1,
    )
    started = time.perf_counter()
    try:
        results = await crawler.crawl(
            ["https://books.toscrape.com/"], max_pages=10, same_domain_only=True,
        )
    finally:
        await crawler.close()
    elapsed = time.perf_counter() - started

    stats = crawler.get_rate_stats()
    robots_info = crawler.robots._cache.get("books.toscrape.com", {})
    print(f"  robots.txt сайта   : {robots_info.get('mode')} (HTTP {robots_info.get('status')})")
    print(f"  Обработано страниц : {len(results)} за {elapsed:.1f} c")
    print(f"  Средняя скорость   : {stats['total_requests'] / elapsed:.2f} запросов/с")
    print(f"  Средняя пауза      : {stats['avg_interval']:.2f} c")
    print(f"  Запрещено robots   : {stats['blocked_by_robots']}")

    header("6б. robots.txt настоящей Википедии")

    # Википедия требует, чтобы робот честно представлялся и оставлял
    # способ связи. Безымянные запросы она отклоняет с кодом 403.
    ua = ("MyBot/1.0 (educational project; "
          "+https://github.com/DmitryAD/project-Concurrency-and-asynchrony)")
    crawler = AsyncCrawler(user_agent=ua)
    try:
        info = await crawler.robots.fetch_robots("https://en.wikipedia.org/")
    finally:
        await crawler.close()

    print(f"\n  User-Agent : {ua}")
    print(f"  Ответ      : HTTP {info['status']}, режим разбора: {info['mode']}")

    if info["status"] in (401, 403):
        print("\n    Сайт отказал в доступе даже к robots.txt.")
        print("    По стандарту ответ 4xx означает «правил нет, можно всё»,")
        print("    но 401/403 на деле значат «тебя сюда не пускают».")
        print("    Обходить такой сайт не стоит, даже если формально можно.")
    elif info["mode"] == "rules":
        print()
        for path in ("/wiki/Python_(programming_language)", "/wiki/Special:Random", "/w/index.php"):
            ok = crawler.robots.can_fetch(f"https://en.wikipedia.org{path}", ua)
            print(f"    {path:<40} {'можно' if ok else 'НЕЛЬЗЯ'}")
        delay = crawler.robots.get_crawl_delay(ua, "https://en.wikipedia.org/")
        print(f"\n    Crawl-delay для нас: {delay or 'не задан'}")
        print("    Итог зависит от того, что сейчас написано у самой Википедии.")
    else:
        print(f"\n    Правил нет или сайт недоступен (режим {info['mode']}).")


async def main() -> None:
    setup_logging()
    server1, B = start_server()
    server2, B2 = start_server()
    Handler.external = f"{B2}/site/x"
    try:
        await demo_one_domain(B)
        await demo_two_domains(B, B2)
        await demo_robots_parsing()
        await demo_robots_crawl(B)
        await demo_delays(B)
    finally:
        server1.shutdown()
        server2.shutdown()
    await demo_live()
    print("\nГотово.\n")


if __name__ == "__main__":
    asyncio.run(main())