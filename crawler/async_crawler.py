"""
Асинхронный HTTP-клиент и обходчик.

День 1 — загрузка страниц.
День 2 — fetch_and_parse: загрузить и сразу разобрать.
День 3 — crawl: обход по ссылкам через очередь, глубина, фильтры,
         лимиты на домен.
День 4 — вежливость: частота запросов, robots.txt, паузы, User-Agent.
День 5 — классификация ошибок, повторы, circuit breaker.
День 6 — сохранение в JSON, CSV или SQLite (параметр storage).
День 7 — fetch_bytes (sitemap.xml.gz), крючок on_page_done для
         статистики и snapshot() для прогресс-бара.

Всё, что добавлялось после дня 3, по умолчанию выключено, так что
код ранних дней работает как раньше. Вывод логов настраивает
вызывающий код, сам модуль только пишет в свои логгеры.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import re
import time
from collections import Counter
from typing import Callable
from urllib.parse import urlparse

import aiohttp

from crawler.circuit_breaker import CircuitBreaker
from crawler.crawler_queue import CrawlerQueue
from crawler.errors import (
    CrawlerError,
    NetworkError,
    ParseError,
    TransientError,
    classify_exception,
    classify_status,
)
from crawler.html_parser import HTMLParser
from crawler.rate_limiter import RateLimiter
from crawler.retry_strategy import RetryStrategy
from crawler.robots_parser import RobotsParser
from crawler.semaphore_manager import SemaphoreManager
from crawler.storage import DataStorage

# сам по себе ничего не печатает, пока приложение не настроит handler'ы
logger = logging.getLogger(__name__)

# отдельно, чтобы в демо можно было оставить только прогресс
progress_logger = logging.getLogger("crawler.progress")


class AsyncCrawler:
    """
    Асинхронный загрузчик и обходчик веб-страниц.

    Загрузка списка (дни 1-2):
        crawler = AsyncCrawler(max_concurrent=5)
        try:
            pages = await crawler.fetch_urls(urls)
        finally:
            await crawler.close()

    Обход сайта по ссылкам (день 3):
        crawler = AsyncCrawler(max_concurrent=10, max_depth=2)
        results = await crawler.crawl(["https://example.com"], max_pages=50)

    Вежливый обход (день 4):
        crawler = AsyncCrawler(
            max_concurrent=5,
            requests_per_second=2.0,
            respect_robots=True,
            min_delay=0.5,
            user_agent="MyBot/1.0",
        )

    Повторы при ошибках (день 5):
        crawler = AsyncCrawler(retry_strategy=RetryStrategy(max_retries=3))
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        connect_timeout: float = 10.0,
        read_timeout: float = 15.0,
        total_timeout: float = 30.0,
        html_parser: HTMLParser | None = None,
        max_depth: int = 3,
        max_per_domain: int | None = None,
        progress_interval: float = 1.0,
        requests_per_second: float | None = None,
        rate_per_domain: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        error_backoff: float = 0.0,
        respect_robots: bool = False,
        user_agent: str | None = None,
        user_agents: list[str] | None = None,
        retry_strategy: RetryStrategy | None = None,
        circuit_breaker: CircuitBreaker | None = None,
        storage: DataStorage | None = None,
        on_page_done: Callable[[dict], None] | None = None,
    ) -> None:
        """
        max_concurrent — сколько запросов идёт одновременно всего.
        connect/read/total_timeout — таймауты, с (день 1, пункт 7).
        html_parser — свой экземпляр HTMLParser с другими настройками
            (день 2). По умолчанию стандартный.
        max_depth — глубина обхода (день 3): 0 — только стартовые,
            1 — плюс ссылки с них, и так далее.
        max_per_domain — запросов одновременно к одному сайту (день 3).
            None — отдельного лимита нет, работает только max_concurrent.
            Для чужих сайтов разумно 3-5.
        progress_interval — как часто писать прогресс, с.

        День 4, всё выключено по умолчанию:
        requests_per_second — не чаще стольких запросов в секунду, None — без лимита.
        rate_per_domain — True: лимит у каждого домена свой, False: общий.
        min_delay — минимальная пауза между запросами, с.
        jitter — случайная добавка к паузе, 0..jitter с.
        error_backoff — замедление после 429, 5xx и таймаутов: пауза
            удваивается с каждой ошибкой подряд, начиная с этого значения.
        respect_robots — соблюдать robots.txt и Crawl-delay.
        user_agent — как представляться сайтам; по нему же выбираются
            правила robots.txt. None — заголовок aiohttp по умолчанию.
        user_agents — список для ротации по кругу. robots.txt проверяется
            для основного имени (user_agent или первого в списке).

        retry_strategy — правила повторов (день 5). None — каждый URL
            запрашивается один раз.
        circuit_breaker — блокировка домена, который подряд отвечает
            ошибками (день 5). None — без неё.
        storage — куда сохранять страницы (день 6): JSONStorage, CSVStorage,
            SQLiteStorage. Закрывается вместе с краулером в close().
        on_page_done — функция, которую crawl() вызывает после каждой
            страницы, удачной или нет (день 7). Её ошибка не останавливает
            обход, а только пишется в лог.
        """
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.progress_interval = progress_interval
        self.html_parser = html_parser or HTMLParser()

        # connect — установка соединения, sock_read — очередная порция данных, total — всё целиком
        self._timeout = aiohttp.ClientTimeout(
            connect=connect_timeout,
            sock_read=read_timeout,
            total=total_timeout,
        )

        # сессия создаётся лениво: она привязана к event loop, а __init__ вызывается до asyncio.run()
        self._session: aiohttp.ClientSession | None = None

        # день 3: два уровня ограничений — общий и по доменам
        self.semaphores = SemaphoreManager(
            max_concurrent=max_concurrent,
            max_per_domain=max_per_domain,
        )

        # дни 1-2: статус запросов и причины ошибок
        self.successful: int = 0
        self.failed: int = 0
        self.errors: dict[str, str] = {}

        # день 3, пункт 4: состояние обхода
        self.visited_urls: set[str] = set()          # что уже качали
        self.failed_urls: dict[str, str] = {}        # url -> ошибка
        self.processed_urls: dict[str, dict] = {}    # url -> разобранные данные
        self.skipped_by_depth: int = 0
        self.queue_stats: dict = {}                  # итог очереди после crawl()

        # день 4: вежливость
        self.rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            per_domain=rate_per_domain,
            min_delay=min_delay,
            jitter=jitter,
            error_backoff=error_backoff,
        )
        self.respect_robots = respect_robots
        self.robots = RobotsParser(fetcher=self._fetch_robots_text)
        self.user_agent = user_agent or (user_agents[0] if user_agents else None)
        self._ua_cycle = itertools.cycle(user_agents) if user_agents else None
        self.blocked_urls: set[str] = set()          # запрещены robots.txt

        # день 5: повторы и подробности об ошибках
        self.retry_strategy = retry_strategy
        self.circuit_breaker = circuit_breaker
        self.error_details: dict[str, dict] = {}     # url -> тип, код, попытки
        self.error_counts: Counter = Counter()       # итоговые ошибки по типам

        # день 6: сохранение и сведения об ответах
        self.storage = storage
        self.response_info: dict[str, dict] = {}     # url -> код ответа и тип содержимого

        # день 7: крючок для статистики и состояние текущего обхода
        self.on_page_done = on_page_done
        self._queue: CrawlerQueue | None = None
        self._crawl_started: float | None = None
        self._pages_started: int = 0

    # ---------- сессия ----------

    def _ensure_session(self) -> aiohttp.ClientSession:
        """Создаёт сессию при первом обращении."""
        if self._session is None or self._session.closed:
            # TCPConnector — пул соединений: после ответа соединение возвращается в пул
            connector = aiohttp.TCPConnector(limit=self.max_concurrent)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self._timeout,
            )
            logger.debug("Создана сессия, лимит соединений: %d", self.max_concurrent)
        return self._session

    async def close(self) -> None:
        """
        Закрывает сессию и пул соединений. Перед этим дописывает буфер
        хранилища и закрывает его, иначе последние записи потерялись бы.
        """
        if self.storage is not None:
            await self.storage.close()
        if self._session is not None and not self._session.closed:
            await self._session.close()
            logger.debug("Сессия закрыта")
        self._session = None

    # ---------- загрузка ----------

    async def fetch_url(self, url: str) -> str | None:
        """
        Загружает одну страницу: HTML или None, если не удалось.
        Исключения наружу не выходят (обещание дня 1).

        С retry_strategy неудачные попытки повторяются (день 5). Причина
        итоговой неудачи — в self.errors, подробности — в self.error_details.
        """
        # сначала robots.txt: запрещённый адрес не занимает ни семафор, ни окно лимита
        if self.respect_robots and not await self._robots_allows(url):
            self._record_blocked(url)
            return None

        try:
            if self.retry_strategy is None:
                html = await self.fetch_once(url)
            else:
                html = await self._fetch_with_retries(url)
        except CrawlerError as err:
            self._record_error(url, err.reason, err)
            return None

        self.successful += 1
        return html

    async def _fetch_with_retries(self, url: str) -> str:
        """
        Повторы через RetryStrategy. Каждая следующая попытка получает
        таймаут больше (пункт 6): медленный сайт с тем же таймаутом упадёт так же.
        """
        assert self.retry_strategy is not None
        attempt = 0

        async def one_attempt(u: str) -> str:
            nonlocal attempt
            attempt += 1
            scale = self.retry_strategy.timeout_growth ** (attempt - 1)
            return await self.fetch_once(u, timeout_scale=scale)

        return await self.retry_strategy.execute_with_retry(one_attempt, url)

    async def fetch_once(self, url: str, timeout_scale: float = 1.0) -> str:
        """
        Ровно одна попытка загрузки; при неудаче бросает CrawlerError
        (TransientError, PermanentError, NetworkError и т. д.).

        Эту функцию и надо отдавать в RetryStrategy:

            await strategy.execute_with_retry(crawler.fetch_once, url)

        fetch_url не подходит: он возвращает None вместо исключения,
        и стратегия не узнает, что надо повторить.

        timeout_scale — во сколько раз увеличить таймауты этой попытки.

        Порядок внутри: circuit breaker → семафоры → лимит скорости → запрос.
        Лимит скорости стоит последним: если поставить его до семафора,
        запрос получил бы разрешение, постоял бы за слотом и ушёл вплотную
        к следующему.
        """
        session = self._ensure_session()
        domain = urlparse(url).netloc.lower()

        # circuit breaker: заблокированный домен отклоняется без запроса в сеть
        if self.circuit_breaker is not None:
            self.circuit_breaker.check(domain, url)

        # слот домена и глобальный (см. SemaphoreManager.acquire)
        async with self.semaphores.acquire(url):
            # лимит скорости — последний шаг перед отправкой (см. docstring)
            await self.rate_limiter.acquire(domain)
            logger.info("Начинаю загрузку %s", url)

            ua = self._next_user_agent()
            extra: dict = {}
            if ua:
                extra["headers"] = {"User-Agent": ua}
            if timeout_scale != 1.0:
                extra["timeout"] = self._scaled_timeout(timeout_scale)

            try:
                async with session.get(url, **extra) as response:
                    # код и тип содержимого запоминаю до raise_for_status — для сохранения (день 6)
                    self.response_info[url] = {
                        "status_code": response.status,
                        "content_type": response.headers.get("Content-Type"),
                    }
                    response.raise_for_status()
                    html = await response.text()

            # except от частного к общему: ClientResponseError, таймаут, остальные ClientError

            except aiohttp.ClientResponseError as e:
                headers = getattr(e, "headers", None) or {}
                err = classify_status(url, e.status, f"ClientResponseError: HTTP {e.status}",
                                      headers.get("Retry-After"))
                # 429 и 5xx — повод притормозить, 404 — нет
                if isinstance(err, TransientError):
                    self._domain_failed(domain)
                raise err from e

            except asyncio.TimeoutError as e:
                self._domain_failed(domain)
                raise TransientError(url, "TimeoutError: превышен таймаут") from e

            except aiohttp.ClientError as e:
                self._domain_failed(domain)
                raise NetworkError(url, f"{type(e).__name__}: {e}") from e

            except Exception as e:            # noqa: BLE001
                logger.exception("Непредвиденная ошибка на %s", url)
                raise classify_exception(e, url) from e

        self.rate_limiter.report_success(domain)
        if self.circuit_breaker is not None:
            self.circuit_breaker.record_success(domain)
        logger.info("Успешно %s — статус %d, %d символов", url, response.status, len(html))
        return html

    async def fetch_bytes(self, url: str) -> tuple[int, bytes]:
        """
        Скачивает адрес как байты и возвращает (код, тело). День 7, для
        sitemap: файл бывает в gzip, и text() его бы испортил.

        Идёт через те же семафоры, лимит скорости и User-Agent, что и страницы.
        4xx/5xx не исключение — решает вызывающий. Сетевые ошибки и таймауты
        пробрасываются как есть.
        """
        session = self._ensure_session()
        domain = urlparse(url).netloc.lower()
        async with self.semaphores.acquire(url):
            await self.rate_limiter.acquire(domain)
            ua = self._next_user_agent()
            headers = {"User-Agent": ua} if ua else None
            async with session.get(url, headers=headers) as response:
                return response.status, await response.read()

    def _domain_failed(self, domain: str) -> None:
        """Сервер не в порядке: сообщить лимитеру (день 4) и автомату (день 5)."""
        self.rate_limiter.report_error(domain)
        if self.circuit_breaker is not None:
            self.circuit_breaker.record_failure(domain)

    def _scaled_timeout(self, scale: float) -> aiohttp.ClientTimeout:
        t = self._timeout
        return aiohttp.ClientTimeout(
            total=t.total * scale if t.total else None,
            connect=t.connect * scale if t.connect else None,
            sock_read=t.sock_read * scale if t.sock_read else None,
        )

    def _record_error(self, url: str, reason: str, err: CrawlerError | None = None) -> None:
        """Записывает ошибку с URL и типом и пишет её в лог."""
        self.failed += 1
        self.errors[url] = reason
        if err is not None:
            kind = type(err).__name__
            self.error_counts[kind] += 1
            self.error_details[url] = {
                "type": kind,
                "status": err.status,
                "reason": reason,
                "attempts": err.attempts,
            }
            logger.warning("Ошибка на %s — %s: %s (попыток: %d)",
                           url, kind, reason, err.attempts)
        else:
            logger.warning("Ошибка на %s — %s", url, reason)

    def get_error_stats(self) -> dict:
        """Пункт 9: итоговая статистика ошибок."""
        stats: dict = {
            "final_errors_by_type": dict(self.error_counts),
            "failed_urls": len(self.error_details),
            "permanent_error_urls": sorted(
                u for u, d in self.error_details.items() if d["type"] == "PermanentError"
            ),
        }
        if self.retry_strategy is not None:
            stats["retries"] = self.retry_strategy.get_stats()
        if self.circuit_breaker is not None:
            stats["circuit_breaker"] = self.circuit_breaker.get_stats()
        return stats

    # ---------- день 4: robots.txt и User-Agent ----------

    async def _robots_allows(self, url: str) -> bool:
        """
        Загружает robots.txt домена (один раз, дальше из кэша), передаёт
        Crawl-delay в лимитер и проверяет адрес.
        """
        domain = urlparse(url).netloc.lower()
        await self.robots.fetch_robots(url)
        delay = self.robots.get_crawl_delay(self.user_agent or "*", url)
        if delay:
            self.rate_limiter.set_crawl_delay(domain, delay)
        return self.robots.can_fetch(url, self.user_agent or "*")

    async def _fetch_robots_text(self, url: str) -> tuple[int, str]:
        """Загрузка robots.txt через сессию краулера."""
        session = self._ensure_session()
        headers = {"User-Agent": self.user_agent} if self.user_agent else None
        async with session.get(url, headers=headers) as response:
            return response.status, await response.text()

    def _record_blocked(self, url: str) -> None:
        self.blocked_urls.add(url)
        logger.warning("robots.txt запрещает %s — пропускаю", url)

    def _next_user_agent(self) -> str | None:
        if self._ua_cycle is not None:
            return next(self._ua_cycle)
        return self.user_agent

    def get_rate_stats(self) -> dict:
        """Пункт 7: скорость, средняя пауза, число заблокированных."""
        stats = self.rate_limiter.get_stats()
        stats["blocked_by_robots"] = len(self.blocked_urls)
        return stats

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        """
        Параллельно загружает список URL.

        Возвращает {url: html} только для успешных загрузок,
        неудачи остаются в self.errors.
        """
        results = await asyncio.gather(
            *(self.fetch_url(url) for url in urls),
            return_exceptions=True,
        )

        pages: dict[str, str] = {}
        for url, result in zip(urls, results):
            if isinstance(result, BaseException):
                logger.error("Исключение просочилось из fetch_url на %s: %r", url, result)
                continue
            if result is not None:
                pages[url] = result

        logger.info("Загружено %d из %d URL", len(pages), len(urls))
        return pages

    # ---------- день 2: загрузка + разбор ----------

    async def fetch_and_parse(self, url: str) -> dict:
        """
        Загружает страницу и разбирает её. При неудаче — словарь той же
        формы с пустыми полями и заполненным error.
        """
        html = await self.fetch_url(url)

        if html is None:
            return {
                "url": url,
                "title": "",
                "text": "",
                "links": [],
                "metadata": {},
                "images": [],
                "headings": {},
                "tables": [],
                "lists": [],
                "parse_errors": [],
                "error": ("запрещено robots.txt" if url in self.blocked_urls
                          else self.errors.get(url, "не удалось загрузить")),
                **self._response_fields(url),
            }

        try:
            result = await self.html_parser.parse_html(html, url)
        except Exception as e:  # noqa: BLE001
            # ParseError не повторяем: тот же HTML упадёт так же
            err = ParseError(url, f"ParseError: {type(e).__name__}: {e}")
            self._record_error(url, err.reason, err)
            return {"url": url, "title": "", "text": "", "links": [], "metadata": {},
                    "images": [], "headings": {}, "tables": [], "lists": [],
                    "parse_errors": [err.reason], "error": err.reason,
                    **self._response_fields(url)}

        if result["parse_errors"]:
            # частичный разбор — не провал, но считаем
            self.error_counts["ParseError (частично)"] += 1
        result["error"] = None
        result.update(self._response_fields(url))

        # save() не бросает исключений: ошибка записи не остановит обход
        if self.storage is not None:
            await self.storage.save(result)
        return result

    def _response_fields(self, url: str) -> dict:
        """Код ответа и тип содержимого, если запрос дошёл до сервера."""
        info = self.response_info.get(url, {})
        status = info.get("status_code")
        if status is None and url in self.error_details:
            status = self.error_details[url].get("status")
        return {"status_code": status, "content_type": info.get("content_type")}

    # ---------- день 3: обход сайта ----------

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = False,
        exclude_patterns: list[str] | None = None,
        include_patterns: list[str] | None = None,
    ) -> dict[str, dict]:
        """
        Обход от start_urls по найденным ссылкам.

        Возвращает {url: данные} успешных страниц, неудачи — в self.failed_urls.

        Схема «очередь + воркеры»:

            очередь URL  ←── найденные ссылки ───┐
                 │                               │
                 ├──→ воркер 1 ─→ качает, разбирает
                 ├──→ воркер 2 ─→ качает, разбирает
                 └──→ воркер N ─→ качает, разбирает

        Воркеров max_concurrent. Обход заканчивается, когда очередь пуста
        и ни один воркер ничего не обрабатывает — иначе кто-то ещё может
        добавить ссылки.

        Фильтры применяются к найденным ссылкам, стартовые URL идут всегда:
          same_domain_only  — только домены стартовых URL
          exclude_patterns  — регулярки; совпала хоть одна — пропускаем
          include_patterns  — регулярки; если заданы, URL должен совпасть с одной

        Сессия здесь не создаётся: её откроет первый запрос. Иначе она
        открывалась бы и там, где сеть не нужна, и aiohttp ругался бы
        на незакрытую сессию.

        Лимит страниц: между проверкой pages_started и его увеличением нет
        ни одного await, поэтому гонки нет и воркеры не проскочат лимит.
        По той же причине robots.txt (с await) проверяется до этого места.
        """
        queue = CrawlerQueue()
        start_domains = {urlparse(u).netloc.lower() for u in start_urls}
        exclude = [re.compile(p) for p in (exclude_patterns or [])]
        include = [re.compile(p) for p in (include_patterns or [])]

        # стартовые URL — глубина 0 и наивысший приоритет
        for url in start_urls:
            queue.add_url(url, priority=self.max_depth + 1, depth=0)

        def should_follow(link: str) -> bool:
            if same_domain_only and urlparse(link).netloc.lower() not in start_domains:
                return False
            if any(p.search(link) for p in exclude):
                return False
            if include and not any(p.search(link) for p in include):
                return False
            return True

        # взятые в работу страницы — для max_pages (гонки нет, см. docstring)
        pages_started = 0

        async def worker() -> None:
            nonlocal pages_started

            while True:
                url = await queue.get_next()
                if url is None:           # сигнал «работы больше нет»
                    return

                # запрещённое robots.txt не качаем и не тратим на него лимит страниц
                if self.respect_robots:
                    try:
                        allowed = await self._robots_allows(url)
                    except Exception:  # noqa: BLE001
                        allowed = True
                    if not allowed:
                        self._record_blocked(url)
                        queue.mark_skipped(url)
                        continue

                # лимит исчерпан: только отметить, чтобы очередь опустела
                if pages_started >= max_pages:
                    queue.mark_skipped(url)
                    continue
                pages_started += 1
                self._pages_started = pages_started

                depth = queue.get_depth(url)
                self.visited_urls.add(url)

                # ровно одна отметка на URL, иначе queue.join() ждал бы вечно
                try:
                    data = await self.fetch_and_parse(url)
                    data["depth"] = depth
                    self._page_done(data)

                    if data["error"]:
                        self.failed_urls[url] = data["error"]
                        queue.mark_failed(url, data["error"])
                        continue

                    self.processed_urls[url] = data

                    # ссылки добавляю до отметки, иначе очередь на миг опустеет и обход закончится раньше
                    if depth < self.max_depth and pages_started < max_pages:
                        for link in data["links"]:
                            if should_follow(link):
                                # ближние страницы раньше
                                queue.add_url(
                                    link,
                                    priority=self.max_depth - depth,
                                    depth=depth + 1,
                                )
                    elif depth >= self.max_depth:
                        self.skipped_by_depth += len(data["links"])

                    queue.mark_processed(url)

                except Exception as e:  # noqa: BLE001
                    logger.exception("Сбой воркера на %s", url)
                    self.failed_urls[url] = f"{type(e).__name__}: {e}"
                    queue.mark_failed(url, str(e))

        started = time.perf_counter()
        # День 7: чтобы snapshot() мог заглянуть в идущий обход
        self._queue, self._crawl_started, self._pages_started = queue, started, 0
        workers = [asyncio.create_task(worker()) for _ in range(self.max_concurrent)]
        reporter = asyncio.create_task(self._report_progress(queue, started))

        try:
            # Ждём, пока каждый добавленный URL получит отметку
            await queue.join()
        finally:
            # Будим воркеров меткой «конец» и ждём их выхода
            queue.close(len(workers))
            await asyncio.gather(*workers, return_exceptions=True)
            reporter.cancel()
            self.queue_stats = queue.get_stats()
            # День 6: всё накопленное в буфере — на диск сразу после обхода
            if self.storage is not None:
                await self.storage.flush()
            self._log_progress(queue, started, final=True)

        return self.processed_urls

    # ---------- день 7: крючок и снимок состояния ----------

    def _page_done(self, data: dict) -> None:
        """Вызывает on_page_done, не давая его ошибке сломать обход."""
        if self.on_page_done is None:
            return
        try:
            self.on_page_done(data)
        except Exception:  # noqa: BLE001
            logger.exception("Ошибка в on_page_done на %s", data.get("url"))

    def snapshot(self) -> dict:
        """
        Состояние обхода прямо сейчас — для прогресс-бара (день 7).
        Ничего не меняет и не ждёт, можно вызывать сколько угодно часто.
        """
        q = self._queue.get_stats() if self._queue is not None else {}
        done = q.get("processed", 0) + q.get("failed", 0)
        elapsed = (time.perf_counter() - self._crawl_started
                   if self._crawl_started is not None else 0.0)
        return {
            "done": done,
            "successful": q.get("processed", 0),
            "failed": q.get("failed", 0),
            "queued": q.get("queued", 0),
            "started": self._pages_started,
            "in_progress": max(self._pages_started - done, 0),
            "active_requests": self.semaphores.get_stats()["active_total"],
            "blocked": len(self.blocked_urls),
            "elapsed": elapsed,
        }

    # ---------- день 3: прогресс ----------

    async def _report_progress(self, queue: CrawlerQueue, started: float) -> None:
        """
        Фоновая задача: раз в progress_interval секунд пишет прогресс.
        Ещё одна корутина в том же event loop, отменяется в конце обхода.
        """
        while True:
            await asyncio.sleep(self.progress_interval)
            self._log_progress(queue, started)

    def _log_progress(self, queue: CrawlerQueue, started: float, final: bool = False) -> None:
        elapsed = time.perf_counter() - started
        stats = queue.get_stats()
        done = stats["processed"] + stats["failed"]
        speed = done / elapsed if elapsed > 0 else 0.0
        rate = self.rate_limiter.get_stats()
        progress_logger.info(
            "%s обработано: %d | в очереди: %d | ошибок: %d | "
            "запрещено robots: %d | активно: %d | %.1f стр/с | "
            "пауза %.2f c | %.1f c",
            "ИТОГ  " if final else "Прогресс",
            stats["processed"], stats["queued"], stats["failed"],
            len(self.blocked_urls),
            self.semaphores.get_stats()["active_total"], speed,
            rate["avg_interval"], elapsed,
        )