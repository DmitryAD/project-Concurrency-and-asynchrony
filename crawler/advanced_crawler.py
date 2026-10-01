"""
День 7, пункт 8 — AdvancedCrawler: всё вместе.

Сам он почти ничего не умеет — он СОБИРАЕТ то, что сделано за неделю,
по настройкам из конфигурации:

    конфигурация (config.yaml)
        │
        ▼
    AdvancedCrawler
        ├── логирование: файл + консоль, ротация      (день 7)
        ├── SitemapParser: стартовые адреса из sitemap (день 7)
        ├── AsyncCrawler: очередь и воркеры             (дни 1-3)
        │     ├── RateLimiter, robots.txt               (день 4)
        │     ├── RetryStrategy, CircuitBreaker         (день 5)
        │     └── хранилище JSON / CSV / SQLite         (день 6)
        ├── CrawlerStats: статистика по каждой странице (день 7)
        └── ProgressMonitor: прогресс-бар               (день 7)

Использование — ровно как в задании:

    crawler = AdvancedCrawler.from_config("config.yaml")
    await crawler.crawl()
    stats = crawler.get_stats()
    crawler.export_to_html_report("report.html")
    await crawler.close()
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TextIO
from urllib.parse import urlparse

from crawler.async_crawler import AsyncCrawler
from crawler.circuit_breaker import CircuitBreaker
from crawler.config import ConfigError, CrawlerConfig, StorageConfig
from crawler.logging_setup import setup_logging
from crawler.progress import ProgressMonitor
from crawler.retry_strategy import RetryStrategy
from crawler.sitemap_parser import SitemapParser
from crawler.stats import CrawlerStats
from crawler.storage import CSVStorage, DataStorage, JSONStorage, SQLiteStorage

logger = logging.getLogger(__name__)

# Расширение файла -> тип хранилища. Для --output в командной строке.
EXTENSION_TYPES = {".json": "json", ".jsonl": "jsonl", ".csv": "csv",
                   ".db": "sqlite", ".sqlite": "sqlite", ".sqlite3": "sqlite"}


def storage_type_for(path: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix not in EXTENSION_TYPES:
        raise ConfigError(f"не знаю, в каком формате сохранять «{path}»: "
                          f"поддерживаются {', '.join(EXTENSION_TYPES)}")
    return EXTENSION_TYPES[suffix]


def make_storage(cfg: StorageConfig) -> DataStorage | None:
    """Хранилище из раздела storage конфигурации."""
    if cfg.type == "none":
        return None
    if cfg.type == "json":
        return JSONStorage(cfg.path, pretty=True, batch_size=cfg.batch_size)
    if cfg.type == "jsonl":
        return JSONStorage(cfg.path, batch_size=cfg.batch_size)
    if cfg.type == "csv":
        # utf-8-sig — чтобы Excel правильно показал кириллицу (день 6)
        return CSVStorage(cfg.path, encoding="utf-8-sig", batch_size=cfg.batch_size)
    if cfg.type == "sqlite":
        return SQLiteStorage(cfg.path, batch_size=cfg.batch_size)
    raise ConfigError(f"неизвестный storage.type: {cfg.type}")


class AdvancedCrawler:
    """
    Краулер, полностью настраиваемый конфигурацией.

    configure_logging — настроить логирование по разделу logging.
        False — оставить как есть (например, в проверках).
    progress_stream — куда рисовать прогресс-бар (по умолчанию stderr).
    """

    def __init__(self, config: CrawlerConfig | None = None, *,
                 configure_logging: bool = True,
                 progress_stream: TextIO | None = None) -> None:
        self.config = config or CrawlerConfig()
        self.config.validate()
        cfg = self.config

        if configure_logging:
            setup_logging(
                level=cfg.logging.level,
                console_level=cfg.logging.console_level,
                log_file=cfg.logging.file,
                max_bytes=cfg.logging.max_bytes,
                backup_count=cfg.logging.backup_count,
                fmt=cfg.logging.format,
            )

        self.storage = make_storage(cfg.storage)
        self.progress_stream = progress_stream or sys.stderr
        self.results: dict[str, dict] = {}
        self.start_urls: list[str] = []
        self.sitemap_urls: list[str] = []
        self.runs = 0                       # сколько раз вызывали crawl()
        self._storage_prepared = False
        self._new_run()

    def _new_run(self) -> None:
        """
        Свежие компоненты для очередного обхода: краулер, статистика, sitemap.

        Хранилище НЕ пересоздаётся: оно одно на весь объект, и второй
        обход дописывает в тот же файл или базу.
        """
        self.stats = CrawlerStats()
        self.crawler = self._build_crawler()
        # Sitemap качается через сам краулер: те же лимиты и User-Agent
        self.sitemap_parser = SitemapParser(fetcher=self.crawler.fetch_bytes,
                                            max_urls=self.config.limits.max_pages)

    def _build_crawler(self) -> AsyncCrawler:
        cfg = self.config
        c = cfg.crawler
        return AsyncCrawler(
            max_concurrent=c.max_concurrent,
            max_per_domain=c.max_per_domain,
            connect_timeout=c.connect_timeout,
            read_timeout=c.read_timeout,
            total_timeout=c.total_timeout,
            max_depth=cfg.limits.max_depth,
            requests_per_second=c.rate_limit,
            rate_per_domain=c.rate_per_domain,
            min_delay=c.min_delay,
            jitter=c.jitter,
            error_backoff=c.error_backoff,
            respect_robots=c.respect_robots,
            user_agent=c.user_agent,
            user_agents=c.user_agents or None,
            retry_strategy=(RetryStrategy(
                max_retries=cfg.retry.max_retries,
                backoff_factor=cfg.retry.backoff_factor,
                base_delay=cfg.retry.base_delay,
                max_delay=cfg.retry.max_delay,
            ) if cfg.retry.enabled else None),
            circuit_breaker=(CircuitBreaker(
                failure_threshold=cfg.circuit_breaker.failure_threshold,
                recovery_timeout=cfg.circuit_breaker.recovery_timeout,
            ) if cfg.circuit_breaker.enabled else None),
            storage=self.storage,
            on_page_done=self._on_page_done,
        )

    @classmethod
    def from_config(cls, source: str | Path | CrawlerConfig | dict, **kwargs) -> "AdvancedCrawler":
        """Из файла YAML/JSON, из словаря или из готового CrawlerConfig."""
        if isinstance(source, CrawlerConfig):
            config = source
        elif isinstance(source, dict):
            config = CrawlerConfig.from_dict(source)
        else:
            config = CrawlerConfig.from_file(source)
        return cls(config, **kwargs)

    # ---------- обход ----------

    async def crawl(self, start_urls: list[str] | None = None) -> dict[str, dict]:
        """
        Запускает обход. Адреса берутся из аргумента, а если его нет —
        из конфигурации, плюс всё найденное в sitemap.

        Возвращает {url: данные страницы} для успешных страниц.

        Каждый вызов — отдельный обход со своей статистикой: второй
        вызов не прибавляет цифры к первому и заново качает страницы.
        Хранилище общее: второй обход дописывает в тот же файл.
        """
        cfg = self.config
        if self.runs > 0:
            await self._restart()
        self.runs += 1
        self._prepare_storage()
        self.stats.start()

        urls = list(start_urls if start_urls is not None else cfg.start_urls)
        self.sitemap_urls = await self._collect_sitemap_urls(urls)
        self.start_urls = list(dict.fromkeys(urls + self.sitemap_urls))
        if not self.start_urls:
            raise ConfigError("нет стартовых адресов: задай start_urls, sitemaps или --urls")

        logger.info("Старт: %d адресов (%d из sitemap), max_pages=%d, max_depth=%d",
                    len(self.start_urls), len(self.sitemap_urls),
                    cfg.limits.max_pages, cfg.limits.max_depth)

        monitor = None
        if cfg.progress.enabled:
            monitor = ProgressMonitor(self.crawler.snapshot, cfg.limits.max_pages,
                                      interval=cfg.progress.interval, stream=self.progress_stream)
            monitor.start()
        try:
            self.results = await self.crawler.crawl(
                self.start_urls,
                max_pages=cfg.limits.max_pages,
                same_domain_only=cfg.filters.same_domain_only,
                include_patterns=cfg.filters.include_patterns or None,
                exclude_patterns=cfg.filters.exclude_patterns or None,
            )
        finally:
            if monitor is not None:
                await monitor.stop()
            self.stats.blocked_by_robots = len(self.crawler.blocked_urls)
            self.stats.finish()

        s = self.stats
        logger.info("Готово: %d страниц, успешно %d, ошибок %d, за %.1f c",
                    s.total_pages, s.successful, s.failed, s.duration)

        # Отчёты, заказанные в конфигурации, — сразу после обхода
        if cfg.output.stats_json:
            self.export_to_json(cfg.output.stats_json)
        if cfg.output.html_report:
            self.export_to_html_report(cfg.output.html_report)
        return self.results

    async def _collect_sitemap_urls(self, start_urls: list[str]) -> list[str]:
        sitemaps = list(self.config.sitemaps)
        if self.config.use_sitemap:
            sites = dict.fromkeys(f"{urlparse(u).scheme}://{urlparse(u).netloc}" for u in start_urls)
            for site in sites:
                sitemaps += await self.sitemap_parser.discover(site)

        found: list[str] = []
        for sitemap in dict.fromkeys(sitemaps):
            found += await self.sitemap_parser.fetch_sitemap(sitemap)
        found = list(dict.fromkeys(found))[: self.config.limits.max_pages]
        if sitemaps:
            logger.info("Из sitemap получено %d адресов", len(found))
        return found

    async def _restart(self) -> None:
        """Закрыть сессию прошлого обхода (но не хранилище) и собрать всё заново."""
        old = self.crawler
        old.storage = None              # иначе close() закрыл бы общее хранилище
        await old.close()
        self._new_run()
        logger.info("Новый обход: статистика прошлого обнулена")

    def _on_page_done(self, data: dict) -> None:
        """Крючок AsyncCrawler: вызывается после каждой страницы."""
        info = self.crawler.error_details.get(data.get("url"), {})
        self.stats.record_page(data, error_type=info.get("type"))

    def _prepare_storage(self) -> None:
        """overwrite: true — каждый запуск начинает файл заново."""
        cfg = self.config.storage
        if self._storage_prepared or cfg.type == "none" or not cfg.overwrite:
            return
        Path(cfg.path).unlink(missing_ok=True)
        self._storage_prepared = True

    # ---------- статистика и отчёты ----------

    def get_stats(self) -> dict:
        """
        Итоговая статистика. Ключи из задания — total_pages, successful,
        failed — на верхнем уровне, подробности по компонентам ниже.
        """
        stats = self.stats.get_stats()
        stats["start_urls"] = self.start_urls
        stats["from_sitemap"] = len(self.sitemap_urls)
        stats["failed_urls_detail"] = {
            url: self.crawler.error_details.get(url, {"reason": reason})
            for url, reason in self.crawler.failed_urls.items()
        }
        stats["components"] = {
            "queue": self.crawler.queue_stats,
            "concurrency": self.crawler.semaphores.get_stats(),
            "rate": self.crawler.get_rate_stats(),
            "errors": self.crawler.get_error_stats(),
            "storage": self.storage.get_stats() if self.storage is not None else None,
            "sitemap": {"files": self.sitemap_parser.fetched,
                        "errors": self.sitemap_parser.errors},
        }
        return stats

    def export_to_json(self, filename: str | Path | None = None) -> Path:
        """Статистика в JSON-файл."""
        path = filename or self.config.output.stats_json or "stats.json"
        extra = {k: v for k, v in self.get_stats().items() if k not in self.stats.get_stats()}
        out = self.stats.export_to_json(path, extra=extra)
        logger.info("Статистика сохранена: %s", out)
        return out

    def export_to_html_report(self, filename: str | Path | None = None,
                              title: str = "Отчёт краулера") -> Path:
        """HTML-отчёт с графиками и таблицами."""
        path = filename or self.config.output.html_report or "report.html"
        extra = {k: v for k, v in self.get_stats().items() if k not in self.stats.get_stats()}
        out = self.stats.export_to_html_report(path, title=title, extra=extra)
        logger.info("HTML-отчёт сохранён: %s", out)
        return out

    # ---------- завершение ----------

    async def close(self) -> None:
        """Дописывает хранилище и закрывает сетевую сессию."""
        await self.crawler.close()

    async def __aenter__(self) -> "AdvancedCrawler":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()