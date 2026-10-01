"""
День 4. Разбор и соблюдение robots.txt.

robots.txt — файл в корне сайта (site.com/robots.txt), где владелец
пишет, куда роботам можно, а куда нельзя. Пример:

    User-agent: *
    Disallow: /admin
    Allow: /admin/public
    Crawl-delay: 2

    User-agent: BadBot
    Disallow: /

Правила разбора — по стандарту RFC 9309:
  - группы начинаются со строк User-agent; робот выбирает группу
    с самым точным совпадением имени, иначе группу "*";
  - из всех Allow/Disallow, подходящих под путь, побеждает САМОЕ
    ДЛИННОЕ правило; при равной длине — Allow;
  - в правилах работают * (любые символы) и $ (конец адреса);
  - нет файла (ответ 4xx) — можно всё;
  - сервер недоступен (5xx, сетевая ошибка) — нельзя ничего:
    раз не можем узнать правила, считаем, что всё запрещено;
  - сам /robots.txt разрешён всегда.

Crawl-delay в стандарт не входит, но многие сайты его пишут,
и вежливые роботы его соблюдают.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Awaitable, Callable
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Функция загрузки: получает адрес, возвращает (HTTP-код, текст).
# Краулер передаёт свою, чтобы robots.txt шёл через его сессию.
Fetcher = Callable[[str], Awaitable[tuple[int, str]]]


class RobotsParser:
    """
        robots = RobotsParser(fetcher=my_fetch)
        await robots.fetch_robots("https://site.com")
        robots.can_fetch("https://site.com/admin", "MyBot/1.0")   # False
        robots.get_crawl_delay("MyBot/1.0", "https://site.com")   # 2.0
    """

    def __init__(self, fetcher: Fetcher | None = None) -> None:
        self._fetcher = fetcher or _default_fetcher
        self._cache: dict[str, dict] = {}                 # домен -> правила
        self._locks: dict[str, asyncio.Lock] = {}
        self._last_domain: str | None = None

    # ---------- загрузка ----------

    @staticmethod
    def _domain(url: str) -> str:
        return urlparse(url).netloc.lower()

    async def fetch_robots(self, base_url: str) -> dict:
        """
        Загружает и разбирает robots.txt домена. Результат кэшируется:
        для каждого домена файл скачивается ровно один раз.

        Замок на домен нужен, потому что на новый домен разом могут прийти
        десять воркеров. Без замка каждый скачал бы robots.txt сам.
        """
        domain = self._domain(base_url)
        if domain in self._cache:
            return self._cache[domain]

        if domain not in self._locks:
            self._locks[domain] = asyncio.Lock()

        async with self._locks[domain]:
            if domain in self._cache:           # пока ждали замок, скачал другой
                return self._cache[domain]

            scheme = urlparse(base_url).scheme or "https"
            robots_url = f"{scheme}://{domain}/robots.txt"
            try:
                status, text = await self._fetcher(robots_url)
            except Exception as e:  # noqa: BLE001
                logger.warning("robots.txt недоступен (%s): %s — считаем, что всё запрещено",
                               robots_url, e)
                rules = self._make(robots_url, None, "disallow_all")
            else:
                if 200 <= status < 300:
                    rules = self.parse(text, robots_url)
                    rules["status"] = status
                elif 400 <= status < 500:
                    logger.info("robots.txt нет (%s, HTTP %d) — ограничений нет", robots_url, status)
                    rules = self._make(robots_url, status, "allow_all")
                else:
                    logger.warning("robots.txt: HTTP %d (%s) — считаем, что всё запрещено",
                                   status, robots_url)
                    rules = self._make(robots_url, status, "disallow_all")

            self._cache[domain] = rules
            self._last_domain = domain
            return rules

    @staticmethod
    def _make(url: str, status: int | None, mode: str) -> dict:
        return {"url": url, "status": status, "mode": mode, "groups": {}, "sitemaps": []}

    # ---------- разбор текста ----------

    def parse(self, text: str, robots_url: str = "") -> dict:
        """
        Разбирает текст robots.txt. Можно вызывать и без сети — удобно
        для проверок. Результат кладётся в кэш для домена robots_url.
        """
        groups: dict[str, dict] = {}
        sitemaps: list[str] = []
        current_agents: list[str] = []
        last_was_agent = False

        for raw in text.splitlines():
            line = raw.split("#", 1)[0].strip()      # отрезаем комментарии
            if ":" not in line:
                continue
            field, value = line.split(":", 1)
            field, value = field.strip().lower(), value.strip()

            if field == "user-agent":
                # Несколько User-agent подряд — одна общая группа.
                # User-agent после правил — начало новой группы.
                if not last_was_agent:
                    current_agents = []
                agent = value.lower()
                current_agents.append(agent)
                groups.setdefault(agent, {"allow": [], "disallow": [], "crawl_delay": None})
                last_was_agent = True
                continue

            last_was_agent = False
            if field == "sitemap":
                sitemaps.append(value)
                continue
            if not current_agents:
                continue          # правило до первого User-agent — игнорируем

            for agent in current_agents:
                group = groups[agent]
                if field in ("allow", "disallow"):
                    if value:     # пустой Disallow означает «можно всё»
                        group[field].append(value)
                elif field == "crawl-delay":
                    try:
                        group["crawl_delay"] = float(value)
                    except ValueError:
                        pass

        rules = {"url": robots_url, "status": 200, "mode": "rules",
                 "groups": groups, "sitemaps": sitemaps}
        if robots_url:
            domain = self._domain(robots_url)
            self._cache[domain] = rules
            self._last_domain = domain
        return rules

    # ---------- проверки ----------

    @staticmethod
    def _select_group(rules: dict, user_agent: str) -> dict | None:
        """Самая точная группа для робота, иначе "*"."""
        ua = (user_agent or "*").lower()
        best, best_len = None, -1
        for agent, group in rules["groups"].items():
            if agent != "*" and agent in ua and len(agent) > best_len:
                best, best_len = group, len(agent)
        return best if best is not None else rules["groups"].get("*")

    @staticmethod
    def _to_regex(pattern: str) -> re.Pattern:
        """ '/private*.pdf$' -> ^/private.*\\.pdf$ """
        anchored = pattern.endswith("$")
        body = pattern[:-1] if anchored else pattern
        regex = "".join(".*" if ch == "*" else re.escape(ch) for ch in body)
        return re.compile("^" + regex + ("$" if anchored else ""))

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """
        Можно ли роботу user_agent запрашивать url.

        Правила домена должны быть заранее загружены через fetch_robots
        (или parse). Если их нет — отвечаем True: метод синхронный
        и сходить за файлом сам не может.
        """
        parts = urlparse(url)
        rules = self._cache.get(parts.netloc.lower())
        if rules is None:
            logger.debug("robots.txt для %s не загружен — разрешаем", parts.netloc)
            return True
        if parts.path == "/robots.txt":
            return True
        if rules["mode"] == "allow_all":
            return True
        if rules["mode"] == "disallow_all":
            return False

        group = self._select_group(rules, user_agent)
        if group is None:
            return True

        target = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")

        best_len, allowed = -1, True
        for kind in ("disallow", "allow"):
            for pattern in group[kind]:
                if self._to_regex(pattern).match(target):
                    length = len(pattern)
                    # Длиннее — сильнее. При равной длине Allow бьёт Disallow,
                    # поэтому allow проверяем вторым и сравниваем через >=.
                    if length > best_len or (length == best_len and kind == "allow"):
                        best_len, allowed = length, (kind == "allow")
        return allowed

    def get_crawl_delay(self, user_agent: str = "*", url: str | None = None) -> float:
        """
        Crawl-delay для робота, в секундах (0.0, если не задан).

        url — любой адрес на нужном домене. Если не указан, берётся
        домен, чей robots.txt загружали последним.
        """
        domain = self._domain(url) if url else self._last_domain
        rules = self._cache.get(domain or "")
        if not rules or rules["mode"] != "rules":
            return 0.0
        group = self._select_group(rules, user_agent)
        if group and group["crawl_delay"]:
            return group["crawl_delay"]
        return 0.0


async def _default_fetcher(url: str) -> tuple[int, str]:
    """Загрузка без краулера — своя короткая сессия aiohttp."""
    import aiohttp

    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as response:
            return response.status, await response.text()