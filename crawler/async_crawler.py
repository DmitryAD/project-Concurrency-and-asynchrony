"""
Асинхронный HTTP-клиент.

День 1 — загрузка страниц.
День 2 — метод fetch_and_parse: загрузить и сразу разобрать.
День 3 — метод crawl: обход сайта по ссылкам через очередь,
         с ограничением глубины, фильтрами и лимитами на домен.
День 4 — вежливость: ограничение частоты запросов, robots.txt,
         паузы, User-Agent. По умолчанию всё выключено — поведение
         дней 1-3 не меняется, пока не включишь явно.
День 5 — классификация ошибок, автоматические повторы, circuit breaker.
День 6 — сохранение результатов в JSON, CSV или SQLite (параметр storage).
         Тоже выключено по умолчанию: без retry_strategy каждый URL
         запрашивается ровно один раз, как в днях 1-4.

Настройка логирования и запуск — в демо-скриптах: библиотека пишет
в логгер, но не решает за приложение, куда и в каком формате выводить.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import re
import time
from collections import Counter
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

# Логгер по имени модуля. Сам по себе ничего не печатает, пока
# приложение не настроит handler'ы — стандартная практика для библиотек.
logger = logging.getLogger(__name__)

# Отдельный логгер для прогресса, чтобы в демо можно было оставить
# только его, приглушив построчные сообщения о каждой загрузке.
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
    ) -> None:
        """
        max_concurrent — сколько запросов летит одновременно всего.

        Таймауты вынесены в параметры, чтобы их можно было проверить
        в демо (день 1, пункт 7: «протестировать таймауты»).

        html_parser — день 2. Свой экземпляр нужен, если хочешь другие
        настройки разбора. Не передашь — создастся стандартный.

        max_depth — день 3. Насколько далеко уходить от стартовой
        страницы: 0 — только она сама, 1 — плюс страницы по ссылкам
        с неё, 2 — плюс ссылки с тех страниц, и так далее.

        max_per_domain — день 3. Сколько запросов одновременно
        к одному сайту. None (по умолчанию) — отдельного лимита нет,
        работает только max_concurrent, как в днях 1-2. Для вежливого
        обхода чужих сайтов задавай явно, например 3-5.

        progress_interval — раз в сколько секунд печатать прогресс.

        День 4 (всё выключено по умолчанию):
        requests_per_second — не чаще стольких запросов в секунду.
            None — без ограничения.
        rate_per_domain — True: лимит у каждого домена свой.
            False: один лимит на все запросы.
        min_delay — минимальная пауза между запросами, секунды.
        jitter — случайная добавка к паузе, от 0 до jitter секунд.
        error_backoff — замедление после ошибок сервера (429, 5xx,
            таймауты): пауза растёт вдвое с каждой ошибкой подряд,
            начиная с этого значения. 0 — выключено.
        respect_robots — проверять robots.txt: не ходить туда,
            где запрещено, и соблюдать Crawl-delay.
        user_agent — как краулер представляется сайтам. По нему же
            выбираются правила в robots.txt. None — стандартный
            заголовок aiohttp.
        user_agents — список для ротации: каждый запрос берёт
            следующий по кругу. Правила robots.txt при этом всё
            равно проверяются для основного имени — первого в списке.

        День 5 (выключено по умолчанию):
        retry_strategy — правила повторов при ошибках. None — без
            повторов, каждый URL запрашивается один раз.
        storage — день 6. Куда сохранять разобранные страницы: JSONStorage,
            CSVStorage, SQLiteStorage. None — ничего не сохранять.
            Хранилище закрывается вместе с краулером в close().

        circuit_breaker — автомат, временно блокирующий домен, который
            подряд отвечает ошибками. None — без автомата.
        """
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.progress_interval = progress_interval
        self.html_parser = html_parser or HTMLParser()

        # ClientTimeout — несколько РАЗНЫХ таймаутов, и это не придирка:
        #   connect   — сколько ждём установления соединения
        #   sock_read — сколько ждём очередную порцию данных из сокета
        #   total     — потолок на всю операцию целиком
        self._timeout = aiohttp.ClientTimeout(
            connect=connect_timeout,
            sock_read=read_timeout,
            total=total_timeout,
        )

        # Сессию создаём лениво, при первом обращении: она привязывается
        # к работающему event loop, а __init__ вызывается до asyncio.run().
        self._session: aiohttp.ClientSession | None = None

        # День 3: вместо одного семафора — менеджер с двумя уровнями
        # ограничений, глобальным и по доменам.
        self.semaphores = SemaphoreManager(
            max_concurrent=max_concurrent,
            max_per_domain=max_per_domain,
        )

        # Счётчики для дней 1-2: статус запросов и причины ошибок.
        self.successful: int = 0
        self.failed: int = 0
        self.errors: dict[str, str] = {}

        # День 3, пункт 4: состояние обхода.
        self.visited_urls: set[str] = set()          # что уже качали
        self.failed_urls: dict[str, str] = {}        # url -> ошибка
        self.processed_urls: dict[str, dict] = {}    # url -> разобранные данные
        self.skipped_by_depth: int = 0
        self.queue_stats: dict = {}                  # итог очереди после crawl()

        # День 4: вежливость.
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

        # День 5: повторы и подробности об ошибках.
        self.retry_strategy = retry_strategy
        self.circuit_breaker = circuit_breaker
        self.error_details: dict[str, dict] = {}     # url -> тип, код, попытки
        self.error_counts: Counter = Counter()       # итоговые ошибки по типам

        # День 6: сохранение и сведения об ответах.
        self.storage = storage
        self.response_info: dict[str, dict] = {}     # url -> код ответа и тип содержимого

    # ---------- сессия ----------

    def _ensure_session(self) -> aiohttp.ClientSession:
        """Создаёт сессию при первом обращении."""
        if self._session is None or self._session.closed:
            # TCPConnector — это и есть connection pooling из дня 1.
            # Соединения не закрываются после ответа, а складываются
            # в пул и переиспользуются.
            connector = aiohttp.TCPConnector(limit=self.max_concurrent)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self._timeout,
            )
            logger.debug("Создана сессия, лимит соединений: %d", self.max_concurrent)
        return self._session

    async def close(self) -> None:
        """
        Закрывает сессию и освобождает соединения пула.

        День 6: заодно дописывает буфер хранилища и закрывает его.
        Без этого последние несколько записей остались бы в памяти
        и пропали.
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
        Загружает одну страницу.

        Возвращает HTML или None, если запрос не удался. Исключения
        наружу не пробрасываются — это обещание дня 1 остаётся в силе.

        День 5: если задана retry_strategy, неудачные попытки
        повторяются по её правилам. Причина окончательной неудачи
        остаётся в self.errors, подробности — в self.error_details.
        """
        # День 4: сначала robots.txt. Запрещённый адрес не занимает
        # ни слот семафора, ни окно в лимите скорости, и не повторяется.
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
        таймаут больше предыдущего (пункт 6): если сайт отвечает
        медленно, ещё одна попытка с тем же таймаутом упадёт так же.
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
        Ровно одна попытка загрузки. При неудаче БРОСАЕТ ошибку одного
        из типов CrawlerError — TransientError, PermanentError,
        NetworkError и т. д.

        Именно эту функцию можно передавать в RetryStrategy напрямую:

            await strategy.execute_with_retry(crawler.fetch_once, url)

        fetch_url для этого не подходит: он ошибки не бросает, а
        возвращает None, и стратегия не поймёт, что нужно повторить.

        timeout_scale — во сколько раз увеличить таймауты этой попытки.
        """
        session = self._ensure_session()
        domain = urlparse(url).netloc.lower()

        # День 5: автомат-предохранитель. Если домен «выбит», запрос
        # отклоняется сразу, не уходя в сеть.
        if self.circuit_breaker is not None:
            self.circuit_breaker.check(domain, url)

        # День 3: слот занимается сразу на двух уровнях — у домена
        # и глобально. Подробности — в SemaphoreManager.acquire.
        async with self.semaphores.acquire(url):
            # День 4: лимит скорости — ПОСЛЕДНИЙ шаг перед отправкой.
            # Если поставить его раньше семафора, запрос мог бы получить
            # разрешение, потом постоять в очереди за слотом и уйти
            # вплотную к следующему — лимит бы нарушался.
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
                    # Превращает HTTP 4xx/5xx в ClientResponseError.
                    # День 6: код ответа и тип содержимого — для сохранения
                    self.response_info[url] = {
                        "status_code": response.status,
                        "content_type": response.headers.get("Content-Type"),
                    }
                    response.raise_for_status()
                    html = await response.text()

            # ВАЖЕН ПОРЯДОК except — от частного к общему:
            #
            #   Exception
            #   └── aiohttp.ClientError
            #       ├── ClientResponseError     ← HTTP 404, 500...
            #       └── ClientConnectionError
            #           └── ServerTimeoutError  ← он же asyncio.TimeoutError

            except aiohttp.ClientResponseError as e:
                headers = getattr(e, "headers", None) or {}
                err = classify_status(url, e.status, f"ClientResponseError: HTTP {e.status}",
                                      headers.get("Retry-After"))
                # 429 «слишком часто» и 5xx «серверу плохо» — сигнал
                # притормозить. 404 — просто нет страницы, это не повод.
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
        """Логировать ошибки с URL и типом ошибки."""
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
        Загружает robots.txt домена (один раз, дальше из кэша),
        передаёт его Crawl-delay в лимитер и проверяет адрес.
        """
        domain = urlparse(url).netloc.lower()
        await self.robots.fetch_robots(url)
        delay = self.robots.get_crawl_delay(self.user_agent or "*", url)
        if delay:
            self.rate_limiter.set_crawl_delay(domain, delay)
        return self.robots.can_fetch(url, self.user_agent or "*")

    async def _fetch_robots_text(self, url: str) -> tuple[int, str]:
        """Загрузка самого robots.txt — через ту же сессию краулера."""
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
        Загружает страницу и сразу разбирает её.

        Если загрузка не удалась, возвращается словарь той же формы,
        но с пустыми полями и заполненным ключом error.
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
            # День 5: ParseError. Не повторяем: тот же HTML разберётся
            # с той же ошибкой.
            err = ParseError(url, f"ParseError: {type(e).__name__}: {e}")
            self._record_error(url, err.reason, err)
            return {"url": url, "title": "", "text": "", "links": [], "metadata": {},
                    "images": [], "headings": {}, "tables": [], "lists": [],
                    "parse_errors": [err.reason], "error": err.reason,
                    **self._response_fields(url)}

        if result["parse_errors"]:
            # Разобрали частично — это не провал, но учесть стоит
            self.error_counts["ParseError (частично)"] += 1
        result["error"] = None
        result.update(self._response_fields(url))

        # День 6: автоматическое сохранение после обработки страницы.
        # save() не бросает исключений: ошибка записи не остановит обход.
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
        Обходит сайт, начиная со start_urls и переходя по найденным ссылкам.

        Возвращает {url: разобранные данные} для успешно обработанных
        страниц. Неудачи — в self.failed_urls.

        Устройство — классическая схема «очередь + воркеры»:

            очередь URL  ←── найденные ссылки ───┐
                 │                               │
                 ├──→ воркер 1 ─→ качает, разбирает
                 ├──→ воркер 2 ─→ качает, разбирает
                 └──→ воркер N ─→ качает, разбирает

        Воркеров столько же, сколько max_concurrent. Каждый в цикле
        берёт URL из очереди, обрабатывает, кладёт найденные ссылки
        обратно в очередь. Обход заканчивается, когда очередь пуста
        И ни один воркер ничего не обрабатывает — иначе кто-то из них
        ещё может добавить новые ссылки.

        Фильтры применяются к найденным ссылкам, стартовые URL
        добавляются всегда:
          same_domain_only  — только домены стартовых URL
          exclude_patterns  — регулярки; совпала хоть одна — пропускаем
          include_patterns  — регулярки; если заданы, URL обязан
                              совпасть хотя бы с одной
        """
        # Сессию здесь НЕ создаём: fetch_url создаст её сам при первом
        # запросе. Иначе сессия откроется даже там, где сеть не нужна,
        # и если её потом не закрыть, aiohttp напишет
        # "Unclosed client session".
        queue = CrawlerQueue()
        start_domains = {urlparse(u).netloc.lower() for u in start_urls}
        exclude = [re.compile(p) for p in (exclude_patterns or [])]
        include = [re.compile(p) for p in (include_patterns or [])]

        # Стартовые URL — глубина 0 и самый высокий приоритет
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

        # Счётчик взятых в работу страниц — для лимита max_pages.
        # Гонки тут нет: всё происходит в одном потоке, а между проверкой
        # и увеличением нет ни одного await, значит никто не вклинится.
        pages_started = 0

        async def worker() -> None:
            nonlocal pages_started

            while True:
                url = await queue.get_next()
                if url is None:           # сигнал «работы больше нет»
                    return

                # День 4: запрещённое robots.txt не качаем и не тратим
                # на него лимит страниц. Проверка стоит ДО подсчёта:
                # между проверкой лимита и его увеличением ниже не должно
                # быть ни одного await, иначе воркеры проскочат лимит.
                if self.respect_robots:
                    try:
                        allowed = await self._robots_allows(url)
                    except Exception:  # noqa: BLE001
                        allowed = True
                    if not allowed:
                        self._record_blocked(url)
                        queue.mark_skipped(url)
                        continue

                # Лимит страниц исчерпан — URL не качаем, просто
                # отмечаем, чтобы очередь могла опустеть до конца.
                if pages_started >= max_pages:
                    queue.mark_skipped(url)
                    continue
                pages_started += 1

                depth = queue.get_depth(url)
                self.visited_urls.add(url)

                # На каждый URL — ровно одна отметка в очереди.
                # try/except гарантирует это даже при неожиданной ошибке:
                # без отметки queue.join() ждал бы вечно.
                try:
                    data = await self.fetch_and_parse(url)

                    if data["error"]:
                        self.failed_urls[url] = data["error"]
                        queue.mark_failed(url, data["error"])
                        continue

                    data["depth"] = depth
                    self.processed_urls[url] = data

                    # Новые ссылки добавляются ДО отметки о завершении.
                    # Иначе очередь могла бы на мгновение показаться
                    # пустой, и обход закончился бы раньше времени.
                    if depth < self.max_depth and pages_started < max_pages:
                        for link in data["links"]:
                            if should_follow(link):
                                # Чем мельче глубина, тем выше приоритет:
                                # сначала обходим ближние страницы.
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

    # ---------- день 3: прогресс ----------

    async def _report_progress(self, queue: CrawlerQueue, started: float) -> None:
        """
        Фоновая задача: раз в progress_interval секунд печатает прогресс.

        Работает параллельно с воркерами — ещё одна корутина в том же
        event loop. Отменяется, когда обход закончен.
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