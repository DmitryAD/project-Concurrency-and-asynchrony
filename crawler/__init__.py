"""
Пакет краулера.

День 1 — базовый асинхронный HTTP-клиент.
День 2 — парсинг HTML и извлечение данных.
"""

from crawler.async_crawler import AsyncCrawler
from crawler.html_parser import HTMLParser

__all__ = ["AsyncCrawler", "HTMLParser"]
