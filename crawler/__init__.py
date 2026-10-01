"""
Пакет краулера.

День 1 — базовый асинхронный HTTP-клиент.
День 2 — парсинг HTML и извлечение данных.
День 3 — очередь, управление конкурентностью, обход по ссылкам.
День 4 — ограничение скорости, robots.txt, вежливость.
День 5 — классификация ошибок, повторы, circuit breaker.
День 6 — сохранение в JSON, CSV, SQLite.
"""

from crawler.async_crawler import AsyncCrawler
from crawler.crawler_queue import CrawlerQueue
from crawler.html_parser import HTMLParser
from crawler.circuit_breaker import CircuitBreaker
from crawler.errors import (
    CircuitOpenError,
    CrawlerError,
    NetworkError,
    ParseError,
    PermanentError,
    RateLimitedError,
    TransientError,
)
from crawler.rate_limiter import RateLimiter
from crawler.retry_strategy import RetryStrategy
from crawler.robots_parser import RobotsParser
from crawler.semaphore_manager import SemaphoreManager
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
]