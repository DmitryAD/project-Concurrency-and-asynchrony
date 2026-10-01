"""
День 7, пункт 1 — sitemap.xml.

Sitemap — это файл, в котором сайт сам перечисляет свои страницы.
Вместо того чтобы искать их по ссылкам (день 3), можно взять
готовый список. Два вида файла:

    Обычный sitemap — список страниц:

        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <url><loc>https://example.com/</loc></url>
          <url><loc>https://example.com/about</loc></url>
        </urlset>

    Индексный sitemap — список ДРУГИХ sitemap. У больших сайтов
    страниц миллионы, а в один файл влезает не больше 50 000,
    поэтому их делят на части:

        <sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
          <sitemap><loc>https://example.com/sitemap-blog.xml</loc></sitemap>
          <sitemap><loc>https://example.com/sitemap-shop.xml.gz</loc></sitemap>
        </sitemapindex>

Индекс разбираем рекурсивно: качаем каждую часть, а если часть сама
оказалась индексом — спускаемся ещё глубже. Части качаются
параллельно, через asyncio.gather.

Где искать sitemap: в robots.txt есть строки «Sitemap: <адрес>»,
а если их нет — по традиционному адресу /sitemap.xml.
"""

from __future__ import annotations

import asyncio
import gzip
import logging
import xml.etree.ElementTree as ET
from typing import Awaitable, Callable
from urllib.parse import urljoin, urlparse

from crawler.robots_parser import RobotsParser

logger = logging.getLogger(__name__)

# Функция загрузки: адрес -> (HTTP-код, тело в байтах).
# Подходит AsyncCrawler.fetch_bytes — тогда sitemap качается через
# те же лимиты и тот же User-Agent, что и страницы.
BytesFetcher = Callable[[str], Awaitable[tuple[int, bytes]]]

# Первые два байта любого gzip-файла
GZIP_MAGIC = b"\x1f\x8b"


def _local(tag: str) -> str:
    """'{http://www.sitemaps.org/...}loc' -> 'loc'. Пространство имён нам не важно."""
    return tag.rsplit("}", 1)[-1]


class SitemapParser:
    """
    Загрузка и разбор sitemap.

        parser = SitemapParser(fetcher=crawler.fetch_bytes)
        urls = await parser.fetch_sitemap("https://example.com/sitemap.xml")

    max_depth — насколько глубоко спускаться по вложенным индексам.
        Защита от бесконечной вложенности на кривых сайтах.
    max_urls — сколько адресов собрать максимум. None — без лимита.

    Ошибки (404, битый XML, обрыв сети) не бросаются наружу: такой
    sitemap даёт пустой список, причина остаётся в self.errors.
    """

    def __init__(self, fetcher: BytesFetcher | None = None,
                 max_depth: int = 3, max_urls: int | None = None) -> None:
        self.fetcher = fetcher or _default_fetcher
        self.max_depth = max_depth
        self.max_urls = max_urls
        self.errors: dict[str, str] = {}        # адрес sitemap -> что пошло не так
        self.fetched: list[str] = []            # какие файлы sitemap скачаны
        self._seen: set[str] = set()            # защита от циклов: A -> B -> A

    # ---------- главное ----------

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        """
        Возвращает все адреса страниц из sitemap — без повторов,
        в порядке появления. Индексы раскрываются рекурсивно.
        """
        urls = await self._fetch(sitemap_url, depth=0)
        unique = list(dict.fromkeys(urls))      # убрать повторы, сохранив порядок
        if self.max_urls is not None:
            unique = unique[: self.max_urls]
        logger.info("Sitemap %s: %d адресов из %d файлов",
                    sitemap_url, len(unique), len(self.fetched))
        return unique

    async def discover(self, site_url: str) -> list[str]:
        """
        Находит адреса sitemap сайта: строки Sitemap: в robots.txt,
        а если их нет — стандартный /sitemap.xml.
        """
        async def text_fetcher(url: str) -> tuple[int, str]:
            status, body = await self.fetcher(url)
            return status, body.decode("utf-8", errors="replace")

        robots = RobotsParser(fetcher=text_fetcher)
        try:
            rules = await robots.fetch_robots(site_url)
            found = list(rules.get("sitemaps", []))
        except Exception as e:  # noqa: BLE001
            logger.warning("Не удалось прочитать robots.txt %s: %s", site_url, e)
            found = []
        if not found:
            found = [urljoin(site_url, "/sitemap.xml")]
        return found

    # ---------- внутреннее ----------

    async def _fetch(self, url: str, depth: int) -> list[str]:
        if url in self._seen:
            return []                           # уже были — это цикл
        self._seen.add(url)

        if depth > self.max_depth:
            self.errors[url] = f"глубже max_depth={self.max_depth}"
            return []

        try:
            status, body = await self.fetcher(url)
        except Exception as e:  # noqa: BLE001
            self.errors[url] = f"{type(e).__name__}: {e}"
            logger.warning("Sitemap %s не загрузился: %s", url, self.errors[url])
            return []
        if status != 200:
            self.errors[url] = f"HTTP {status}"
            logger.warning("Sitemap %s: HTTP %d", url, status)
            return []
        self.fetched.append(url)

        try:
            kind, locs = self.parse(body)
        except (ET.ParseError, OSError, EOFError) as e:
            # OSError/EOFError — повреждённый gzip
            self.errors[url] = f"не разобрался: {type(e).__name__}: {e}"
            logger.warning("Sitemap %s: %s", url, self.errors[url])
            return []

        # Относительные адреса встречаются, хоть стандарт и запрещает
        locs = [urljoin(url, loc) for loc in locs]
        locs = [u for u in locs if urlparse(u).scheme in ("http", "https")]

        if kind == "urlset":
            return locs

        # Индекс: все вложенные sitemap — параллельно
        parts = await asyncio.gather(*(self._fetch(u, depth + 1) for u in locs))
        return [u for part in parts for u in part]

    @staticmethod
    def parse(body: bytes) -> tuple[str, list[str]]:
        """
        Разбирает содержимое одного файла sitemap.

        Возвращает (вид, адреса): вид — "urlset" или "sitemapindex".
        Сжатый gzip распознаётся по первым байтам, а не по имени
        файла: сервер может отдать .gz уже распакованным и наоборот.
        """
        if body[:2] == GZIP_MAGIC:
            body = gzip.decompress(body)

        root = ET.fromstring(body)
        kind = _local(root.tag)
        if kind not in ("urlset", "sitemapindex"):
            raise ET.ParseError(f"неизвестный корневой тег <{kind}>")

        locs = []
        for entry in root:                       # <url> или <sitemap>
            for child in entry:
                if _local(child.tag) == "loc" and child.text and child.text.strip():
                    locs.append(child.text.strip())
        return kind, locs


async def _default_fetcher(url: str) -> tuple[int, bytes]:
    """Загрузка без краулера — для использования SitemapParser отдельно."""
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            return response.status, await response.read()