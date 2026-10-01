"""
День 7 — демонстрация (пункт 9 задания).

    pip install -r requirements.txt     # появился pyyaml
    python -m demos.demo_day7

1. Пример из задания: AdvancedCrawler.from_config("config.yaml") —
   обход books.toscrape.com со всеми возможностями недели сразу.
2. Sitemap на локальном тестовом сервере: индекс, gzip, вложенный
   индекс, битая ссылка — и обход по найденным адресам.
3. Что осталось на диске: база, лог с ротацией, статистика, отчёт.

Файлы появятся в output/day7/ — эта папка в .gitignore.
Отчёт откроется в браузере: output/day7/report.html.
"""

import asyncio
from pathlib import Path

from crawler import AdvancedCrawler, CrawlerConfig, SQLiteStorage
from tests.check_all import Handler, start_server

OUT = Path("output") / "day7"


def header(title: str) -> None:
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}")


# ============================================================
# 1. Пример из задания
# ============================================================

async def demo_from_config() -> None:
    header("1. AdvancedCrawler.from_config('config.yaml') — books.toscrape.com")
    print("\n  Ниже — прогресс-бар. Он обновляется на месте, раз в полсекунды.\n")

    crawler = AdvancedCrawler.from_config("config.yaml")

    await crawler.crawl()

    stats = crawler.get_stats()
    print(f"\n  Обработано: {stats['total_pages']} страниц")
    print(f"  Успешно: {stats['successful']}")
    print(f"  Ошибок: {stats['failed']}")

    crawler.export_to_html_report(OUT / "report.html")
    await crawler.close()

    # Дальше — то, что есть в статистике помимо трёх чисел из задания
    print(f"\n  Время работы: {stats['duration_seconds']:.1f} c, "
          f"средняя скорость {stats['pages_per_second']:.2f} стр/с")
    print(f"  Запрещено robots.txt: {stats['blocked_by_robots']}")
    print(f"  Коды ответов: {stats['status_codes']}")
    print(f"  Глубина: {stats['depth_distribution']}")
    print("  Топ доменов:", ", ".join(f"{d['domain']} ({d['pages']})" for d in stats["top_domains"]))
    rate = stats["components"]["rate"]
    print(f"  Средний интервал между запросами: {rate['avg_interval']:.2f} c "
          f"(лимит в config.yaml — 3 в секунду)")
    print(f"  Пик одновременных запросов: {stats['components']['concurrency']['peak_total']} "
          f"(лимит max_concurrent — 5)")
    print(f"\n  HTML-отчёт: {OUT / 'report.html'} — открой в браузере")


# ============================================================
# 2. Sitemap
# ============================================================

async def demo_sitemap() -> None:
    header("2. Sitemap: страницы из sitemap.xml как стартовые адреса")
    server1, B = start_server()
    server2, B2 = start_server()
    Handler.external = f"{B2}/site/x"

    config = CrawlerConfig.from_dict({
        "sitemaps": [f"{B}/sitemap.xml"],
        "limits": {"max_pages": 30, "max_depth": 0},     # 0 — только адреса из sitemap
        "progress": {"enabled": False},
        "output": {"stats_json": str(OUT / "sitemap_stats.json")},
    })
    crawler = AdvancedCrawler(config, configure_logging=False)
    try:
        await crawler.crawl()
        stats = crawler.get_stats()
    finally:
        await crawler.close()
        server1.shutdown()
        server2.shutdown()

    sm = stats["components"]["sitemap"]
    print(f"\n  Скачано файлов sitemap: {len(sm['files'])}")
    for f in sm["files"]:
        print(f"    {f.replace(B, '')}")
    print("  Не удалось:")
    for f, why in sm["errors"].items():
        print(f"    {f.replace(B, ''):<28} {why}")
    print(f"\n  Адресов из sitemap: {stats['from_sitemap']} (без повторов, цикл на сам индекс не страшен)")
    for u in crawler.sitemap_urls:
        print(f"    {u.replace(B, '')}")
    print(f"\n  Обход: успешно {stats['successful']}, ошибок {stats['failed']}")
    for url, info in stats["failed_urls_detail"].items():
        print(f"    {url.replace(B, '')}: {info.get('reason')}")


# ============================================================
# 3. Что на диске
# ============================================================

async def demo_files() -> None:
    header("3. Что осталось в output/day7/")
    print()
    for path in sorted(OUT.iterdir()):
        print(f"  {path.name:<22} {path.stat().st_size / 1024:8.1f} КБ")

    log = OUT / "crawler.log"
    if log.exists():
        lines = log.read_text(encoding="utf-8").splitlines()
        print(f"\n  crawler.log — {len(lines)} строк, уровень DEBUG. Последние три:")
        for line in lines[-3:]:
            print(f"    {line[:110]}")
        print("  В консоли при этом были только WARNING и выше — так задано в config.yaml.")

    db = SQLiteStorage(OUT / "pages.db")
    try:
        rows = await db.query("SELECT COUNT(*) AS n FROM pages")
        print(f"\n  pages.db: {rows[0]['n']} страниц, как и раньше читается через pandas:")
        print("    pd.read_sql('SELECT * FROM pages', sqlite3.connect('output/day7/pages.db'))")
    finally:
        await db.close()

    print("\n  То же самое из командной строки:")
    print("    python -m crawler --config config.yaml")
    print("    python -m crawler --urls https://books.toscrape.com/ --max-pages 20 \\")
    print("        --output output/day7/books.csv --report output/day7/cli_report.html")
    print("    python -m crawler --help")


async def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    await demo_from_config()
    await demo_sitemap()
    await demo_files()
    print("\nГотово.\n")


if __name__ == "__main__":
    asyncio.run(main())