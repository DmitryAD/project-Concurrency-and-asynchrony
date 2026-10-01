"""
День 7, пункт 5 — командная строка.

    python -m crawler --urls https://example.com --max-pages 100 --output results.json

В задании «python crawler.py», но crawler у меня — пакет, и файл
crawler.py рядом конфликтовал бы с ним по имени. «python -m crawler»
запускает crawler/__main__.py, который вызывает main() отсюда.
Параметры те же, что в задании.

Приоритет: значения по умолчанию < файл --config < параметры командной строки.
Например, всё в config.yaml, а одно значение — на лету:

    python -m crawler --config config.yaml --max-pages 10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from crawler.advanced_crawler import AdvancedCrawler, storage_type_for
from crawler.config import ConfigError, CrawlerConfig


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m crawler",
        description="Асинхронный веб-краулер: обход сайта, сохранение, статистика.",
        epilog="Пример: python -m crawler --urls https://books.toscrape.com/ "
               "--max-pages 50 --output output/books.jsonl --report output/report.html",
    )
    p.add_argument("--urls", nargs="+", metavar="URL", help="стартовые адреса")
    p.add_argument("--config", metavar="ФАЙЛ", help="конфигурация .yaml или .json")
    p.add_argument("--max-pages", type=int, metavar="N", help="сколько страниц обработать максимум")
    p.add_argument("--max-depth", type=int, metavar="N", help="глубина обхода от стартовых адресов")
    p.add_argument("--output", metavar="ФАЙЛ",
                   help="куда сохранять страницы; формат по расширению: "
                        ".json, .jsonl, .csv, .db/.sqlite")
    # пара --respect-robots / --no-respect-robots: можно и включить, и выключить
    p.add_argument("--respect-robots", action=argparse.BooleanOptionalAction, default=None,
                   help="соблюдать robots.txt")
    p.add_argument("--rate-limit", type=float, metavar="RPS", help="не больше стольких запросов в секунду")

    extra = p.add_argument_group("дополнительно")
    extra.add_argument("--concurrency", type=int, metavar="N", help="одновременных запросов")
    extra.add_argument("--sitemap", nargs="+", metavar="URL", help="адреса sitemap.xml")
    extra.add_argument("--use-sitemap", action="store_true",
                       help="найти sitemap через robots.txt стартовых сайтов")
    extra.add_argument("--same-domain", action=argparse.BooleanOptionalAction, default=None,
                       help="не уходить на другие домены")
    extra.add_argument("--report", metavar="ФАЙЛ.html", help="сохранить HTML-отчёт")
    extra.add_argument("--stats-json", metavar="ФАЙЛ.json", help="сохранить статистику в JSON")
    extra.add_argument("--log-file", metavar="ФАЙЛ", help="писать лог в файл (с ротацией)")
    extra.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="уровень для файла лога")
    extra.add_argument("--user-agent", metavar="СТРОКА", help="как представляться сайтам")
    extra.add_argument("--user-agents", nargs="+", metavar="СТРОКА",
                       help="несколько User-Agent для ротации: каждый запрос берёт следующий")
    extra.add_argument("--no-progress", action="store_true", help="не показывать прогресс-бар")
    return p


def config_from_args(args: argparse.Namespace) -> CrawlerConfig:
    """Конфигурация из файла (если указан) + поверх неё параметры командной строки."""
    config = CrawlerConfig.from_file(args.config) if args.config else CrawlerConfig()

    if args.urls:
        config.start_urls = args.urls
    if args.sitemap:
        config.sitemaps = args.sitemap
    if args.use_sitemap:
        config.use_sitemap = True
    if args.max_pages is not None:
        config.limits.max_pages = args.max_pages
    if args.max_depth is not None:
        config.limits.max_depth = args.max_depth
    if args.respect_robots is not None:
        config.crawler.respect_robots = args.respect_robots
    if args.rate_limit is not None:
        config.crawler.rate_limit = args.rate_limit
    if args.concurrency is not None:
        config.crawler.max_concurrent = args.concurrency
    if args.same_domain is not None:
        config.filters.same_domain_only = args.same_domain
    if args.user_agent:
        config.crawler.user_agent = args.user_agent
    if args.user_agents:
        config.crawler.user_agents = args.user_agents
    if args.output:
        config.storage.type = storage_type_for(args.output)
        config.storage.path = args.output
    if args.report:
        config.output.html_report = args.report
    if args.stats_json:
        config.output.stats_json = args.stats_json
    if args.log_file:
        config.logging.file = args.log_file
    if args.log_level:
        config.logging.level = args.log_level
    if args.no_progress:
        config.progress.enabled = False

    config.validate()
    if not (config.start_urls or config.sitemaps):
        raise ConfigError("не указано, откуда начинать: --urls, --sitemap или start_urls в --config")
    return config


async def run(config: CrawlerConfig) -> dict:
    crawler = AdvancedCrawler(config)
    try:
        await crawler.crawl()
        return crawler.get_stats()
    finally:
        await crawler.close()


def main(argv: list[str] | None = None) -> int:
    """
    Код выхода: 0 — обход завершён (неудачные страницы есть в статистике),
    2 — неверные параметры, 130 — Ctrl+C.
    """
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = config_from_args(args)
    except ConfigError as e:
        print(f"Ошибка: {e}", file=sys.stderr)
        return 2

    try:
        stats = asyncio.run(run(config))
    except KeyboardInterrupt:
        print("\nОстановлено пользователем (Ctrl+C).", file=sys.stderr)
        return 130

    summary = {k: stats[k] for k in ("total_pages", "successful", "failed",
                                     "blocked_by_robots", "duration_seconds", "pages_per_second")}
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    for label, path in (("Страницы", config.storage.path if config.storage.type != "none" else None),
                        ("HTML-отчёт", config.output.html_report),
                        ("Статистика", config.output.stats_json),
                        ("Лог", config.logging.file)):
        if path:
            print(f"{label}: {path}")
    return 0