"""
Пакет краулера.

День 1 — базовый асинхронный HTTP-клиент.
День 2 — парсинг HTML и извлечение данных.
День 3 — очередь, управление конкурентностью, обход по ссылкам.
"""

from crawler.async_crawler import AsyncCrawler
from crawler.crawler_queue import CrawlerQueue
from crawler.html_parser import HTMLParser
from crawler.semaphore_manager import SemaphoreManager

__all__ = ["AsyncCrawler", "CrawlerQueue", "HTMLParser", "SemaphoreManager"]