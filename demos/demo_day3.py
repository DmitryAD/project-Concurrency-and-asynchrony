"""
День 3 — демонстрация (пункты 8 и 9 задания).

    python -m demos.demo_day3

Пункт 9 — тестирование на ненастоящем сайте, без сети:
    очередь с приоритетами, ограничение глубины, фильтрация URL,
    отсутствие дубликатов, лимиты семафоров.

Пункт 8 — живой обход настоящего сайта books.toscrape.com
    (учебный сайт, сделанный специально для тренировки парсинга),
    с прогрессом в реальном времени и сохранением результатов в JSON.
"""

import asyncio
import json
import logging
from collections import Counter
from pathlib import Path

from crawler import AsyncCrawler, CrawlerQueue


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )
    # построчные сообщения о загрузке скрывают прогресс, оставляем только предупреждения
    logging.getLogger("crawler.async_crawler").setLevel(logging.WARNING)
    logging.getLogger("crawler.html_parser").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def header(title: str) -> None:
    print(f"\n{'=' * 64}\n  {title}\n{'=' * 64}")


S = "https://shop.test"


def page(*links: str) -> str:
    anchors = "".join(f'<a href="{link}">ссылка</a>' for link in links)
    return f"<html><head><title>Страница</title></head><body>{anchors}</body></html>"


FAKE_SITE = {
    f"{S}/": page("/a", "/b", "/c", "https://external.com/x", "/private/login", "/a"),
    f"{S}/a": page("/a/1", "/a/2", "/"),
    f"{S}/b": page("/b/1", "/a"),
    f"{S}/private/login": page(),
    f"{S}/a/1": page("/a/1/deep"),
    f"{S}/a/2": page("/"),
    f"{S}/b/1": page("/b/1/deep"),
    f"{S}/a/1/deep": page(),
    f"{S}/b/1/deep": page(),
    "https://external.com/x": page("/y"),
}


class OfflineCrawler(AsyncCrawler):
    """
    Краулер, который берёт страницы из словаря FAKE_SITE вместо сети.

    Подменён только fetch_url, очередь, глубина, фильтры и семафоры
    работают как в боевом коде. Структура тестового сайта:

        /                  глубина 0
        ├── /a             глубина 1
        │   ├── /a/1       глубина 2
        │   │   └── /a/1/deep   глубина 3
        │   └── /a/2       глубина 2  (ссылается обратно на /)
        ├── /b             глубина 1  (ссылается на /a — повтор)
        │   └── /b/1       глубина 2
        │       └── /b/1/deep   глубина 3
        ├── /c             глубина 1  (не существует → 404)
        ├── /private/login глубина 1  (служебный раздел)
        └── external.com/x глубина 1  (чужой домен)

    Циклы, повторные ссылки, чужой домен и 404 добавлены специально.
    """

    def __init__(self, site: dict[str, str], delay: float = 0.02, **kwargs):
        kwargs.setdefault("progress_interval", 100)   # прогресс не нужен
        super().__init__(**kwargs)
        self.site = site
        self.delay = delay
        self.fetch_count: Counter = Counter()

    async def fetch_url(self, url: str) -> str | None:
        self.fetch_count[url] += 1
        async with self.semaphores.acquire(url):
            await asyncio.sleep(self.delay)          # имитация сети
            html = self.site.get(url)
            if html is None:
                self._record_error(url, "ClientResponseError: HTTP 404")
                return None
            self.successful += 1
            return html


def short(url: str) -> str:
    return url.replace(S, "") or "/"


async def test_queue_priority() -> None:
    """Пункт 9. Очередь с приоритетами."""
    header("Пункт 9. Очередь с приоритетами")

    queue = CrawlerQueue()
    queue.add_url(f"{S}/low", priority=0)
    queue.add_url(f"{S}/high", priority=10)
    queue.add_url(f"{S}/mid", priority=5)
    queue.add_url(f"{S}/high-2", priority=10)
    added_again = queue.add_url(f"{S}/mid", priority=100)   # дубликат

    print("\n  Добавлены: low(0), high(10), mid(5), high-2(10), и ещё раз mid(100)")

    order = [short(await queue.get_next()) for _ in range(4)]
    print(f"  Порядок выдачи: {order}")
    print(f"  Повторное добавление mid принято: {added_again}")

    assert order == ["/high", "/high-2", "/mid", "/low"], order
    assert added_again is False
    print("\n    Больший приоритет выходит раньше, при равном — кто раньше")
    print("    пришёл. Дубликат отвергнут, даже с более высоким приоритетом.")


async def test_depth_limit() -> None:
    """Пункт 9. Ограничение глубины."""
    header("Пункт 9. Ограничение глубины")

    for max_depth, expected in ((0, 1), (1, 6), (2, 10)):
        crawler = OfflineCrawler(FAKE_SITE, max_depth=max_depth)
        await crawler.crawl([f"{S}/"], max_pages=100)

        depths = sorted({d["depth"] for d in crawler.processed_urls.values()})
        print(f"\n  max_depth={max_depth}: посещено {len(crawler.visited_urls)} "
              f"страниц, глубины обработанных: {depths}")

        assert len(crawler.visited_urls) == expected, crawler.visited_urls
        assert max(depths) <= max_depth

    print("\n    Глубже заданного краулер не уходит.")


async def test_filters() -> None:
    """Пункт 9. Фильтрация URL."""
    header("Пункт 9. Фильтрация URL")

    crawler = OfflineCrawler(FAKE_SITE, max_depth=3)
    await crawler.crawl(
        [f"{S}/"],
        same_domain_only=True,
        exclude_patterns=[r"/private"],
    )
    visited = sorted(short(u) for u in crawler.visited_urls)
    print(f"\n  same_domain_only + exclude /private:\n    {visited}")

    assert not any("external.com" in u for u in crawler.visited_urls)
    assert not any("/private" in u for u in crawler.visited_urls)
    assert len(visited) == 9

    crawler = OfflineCrawler(FAKE_SITE, max_depth=3)
    await crawler.crawl([f"{S}/"], same_domain_only=True, include_patterns=[r"/a"])
    visited = sorted(short(u) for u in crawler.visited_urls)
    print(f"\n  include только /a:\n    {visited}")

    assert visited == ["/", "/a", "/a/1", "/a/1/deep", "/a/2"], visited
    print("\n    Чужой домен, служебный раздел и лишние ветки отсечены.")
    print("    Стартовая страница добавляется всегда, фильтры — для найденных ссылок.")


async def test_no_duplicates() -> None:
    """Пункт 9. Отсутствие дубликатов."""
    header("Пункт 9. Отсутствие дубликатов")

    crawler = OfflineCrawler(FAKE_SITE, max_depth=5)
    await crawler.crawl([f"{S}/"])

    repeats = {u: n for u, n in crawler.fetch_count.items() if n > 1}
    print(f"\n  На сайте есть циклы (/a/2 → /, /a → /) и повторы (/b → /a).")
    print(f"  Страниц скачано          : {sum(crawler.fetch_count.values())}")
    print(f"  Уникальных адресов       : {len(crawler.fetch_count)}")
    print(f"  Отвергнуто дубликатов    : {crawler.queue_stats['duplicates_rejected']}")
    print(f"  Скачаны больше одного раза: {repeats or 'нет'}")

    assert not repeats
    assert len(crawler.visited_urls) == sum(crawler.fetch_count.values())
    print("\n    Каждый адрес скачан ровно один раз, циклы не зациклили обход.")


async def test_semaphores() -> None:
    """Пункт 9. Семафоры: лимит на домен и общий лимит."""
    header("Пункт 9. Семафоры: лимит на домен и общий лимит")

    # Широкий сайт: главная ссылается сразу на 20 страниц
    wide = {"https://one.test/": page(*[f"/p{i}" for i in range(20)])}
    wide |= {f"https://one.test/p{i}": page() for i in range(20)}

    crawler = OfflineCrawler(wide, delay=0.1, max_concurrent=10,
                             max_per_domain=3, max_depth=1)
    await crawler.crawl(["https://one.test/"])
    stats = crawler.semaphores.get_stats()
    print(f"\n  Один сайт, max_concurrent=10, max_per_domain=3:")
    print(f"    пик одновременных запросов к сайту: {stats['peak_by_domain']}")
    assert stats["peak_by_domain"]["one.test"] == 3

    # Два сайта, общий лимит меньше суммы доменных
    two = dict(wide)
    two |= {"https://two.test/": page(*[f"/p{i}" for i in range(20)])}
    two |= {f"https://two.test/p{i}": page() for i in range(20)}

    crawler = OfflineCrawler(two, delay=0.1, max_concurrent=4,
                             max_per_domain=3, max_depth=1)
    await crawler.crawl(["https://one.test/", "https://two.test/"])
    stats = crawler.semaphores.get_stats()
    print(f"\n  Два сайта, max_concurrent=4, max_per_domain=3:")
    print(f"    пик всего    : {stats['peak_total']}")
    print(f"    пик по сайтам: {stats['peak_by_domain']}")
    assert stats["peak_total"] <= 4
    assert all(n <= 3 for n in stats["peak_by_domain"].values())

    print("\n    Ни один сайт не получил больше 3 запросов разом,")
    print("    а всего одновременно летело не больше 4.")


async def demo_live_crawl() -> None:
    """Пункт 8. Живой обход настоящего сайта."""
    header("Пункт 8. Обход books.toscrape.com")

    print("\n  Старт: главная страница, max_depth=2, не больше 30 страниц,")
    print("  только свой домен. Прогресс — раз в секунду.\n")

    crawler = AsyncCrawler(max_concurrent=10, max_per_domain=5, max_depth=2)
    try:
        results = await crawler.crawl(
            start_urls=["https://books.toscrape.com/"],
            max_pages=30,
            same_domain_only=True,
        )
    finally:
        await crawler.close()

    by_depth = Counter(d["depth"] for d in results.values())
    print(f"\n  Обработано страниц : {len(results)}")
    print(f"  По глубинам        : {dict(sorted(by_depth.items()))}")
    print(f"  Ошибок             : {len(crawler.failed_urls)}")
    print(f"  Отвергнуто дублей  : {crawler.queue_stats['duplicates_rejected']}")
    print(f"  Осталось в очереди : {crawler.queue_stats['skipped']} (не взяты из-за max_pages)")
    print(f"  Пик запросов к сайту: {crawler.semaphores.get_stats()['peak_by_domain']}")

    print("\n  Первые страницы:")
    for url, data in list(results.items())[:5]:
        print(f"    [глубина {data['depth']}] {data['title'][:50]:<50} {url}")

    # Сохранение найденных данных (пункт 8)
    out = Path("output") / "day3_results.json"
    out.parent.mkdir(exist_ok=True)
    rows = [
        {
            "url": url,
            "depth": data["depth"],
            "title": data["title"],
            "links_count": len(data["links"]),
            "text_length": len(data["text"]),
            "h1": data["headings"].get("h1", []),
        }
        for url, data in results.items()
    ]
    out.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n  Результаты сохранены в {out} ({len(rows)} записей)")


async def main() -> None:
    setup_logging()
    await test_queue_priority()
    await test_depth_limit()
    await test_filters()
    await test_no_duplicates()
    await test_semaphores()
    await demo_live_crawl()
    print("\nГотово.\n")


if __name__ == "__main__":
    asyncio.run(main())