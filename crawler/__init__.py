"""
Пакет краулера.

День 1 — базовый асинхронный HTTP-клиент.
День 2 — парсинг HTML и извлечение данных.
День 3 — очередь, управление конкурентностью, обход по ссылкам.
День 4 — ограничение скорости, robots.txt, вежливость.
День 5 — классификация ошибок, повторы, circuit breaker.
День 6 — сохранение в JSON, CSV, SQLite.
День 7 — sitemap, статистика и отчёты, конфигурация, CLI,
         логирование в файл, прогресс-бар, AdvancedCrawler.
"""

from crawler.advanced_crawler import AdvancedCrawler
from crawler.async_crawler import AsyncCrawler
from crawler.crawler_queue import CrawlerQueue
from crawler.html_parser import HTMLParser
from crawler.circuit_breaker import CircuitBreaker
from crawler.config import ConfigError, CrawlerConfig, load_config
from crawler.errors import (
    CircuitOpenError,
    CrawlerError,
    NetworkError,
    ParseError,
    PermanentError,
    RateLimitedError,
    TransientError,
)
from crawler.logging_setup import setup_logging
from crawler.progress import ProgressMonitor
from crawler.rate_limiter import RateLimiter
from crawler.retry_strategy import RetryStrategy
from crawler.robots_parser import RobotsParser
from crawler.semaphore_manager import SemaphoreManager
from crawler.sitemap_parser import SitemapParser
from crawler.stats import CrawlerStats
from crawler.storage import (
    CSVStorage,
    DataStorage,
    JSONStorage,
    MultiStorage,
    SQLiteStorage,
)

__all__ = [
    "AsyncCrawler", "CrawlerQueue", "HTMLParser",
    "RateLimiter", "RobotsParser", "SemaphoreManager",
    "RetryStrategy", "CircuitBreaker",
    "CrawlerError", "TransientError", "RateLimitedError", "PermanentError",
    "NetworkError", "ParseError", "CircuitOpenError",
    "DataStorage", "JSONStorage", "CSVStorage", "SQLiteStorage", "MultiStorage",
    "AdvancedCrawler", "SitemapParser", "CrawlerStats", "CrawlerConfig", "ConfigError",
    "load_config", "setup_logging", "ProgressMonitor",
]