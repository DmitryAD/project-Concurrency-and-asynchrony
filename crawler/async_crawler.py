"""
Асинхронный HTTP-клиент.

День 1 — загрузка страниц.
День 2 — плюс метод fetch_and_parse: загрузить и сразу разобрать.

Настройка логирования и запуск — в демо-скриптах: библиотека пишет
в логгер, но не решает за приложение, куда и в каком формате выводить.
"""

from __future__ import annotations

import asyncio
import logging

import aiohttp

from crawler.html_parser import HTMLParser

# Логгер по имени модуля. Сам по себе ничего не печатает, пока
# приложение не настроит handler'ы — стандартная практика для библиотек.
logger = logging.getLogger(__name__)


class AsyncCrawler:
    """
    Асинхронный загрузчик веб-страниц.

        crawler = AsyncCrawler(max_concurrent=5)
        try:
            pages = await crawler.fetch_urls(urls)
        finally:
            await crawler.close()
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        connect_timeout: float = 10.0,
        read_timeout: float = 15.0,
        total_timeout: float = 30.0,
        html_parser: HTMLParser | None = None,
    ) -> None:
        """
        max_concurrent — сколько запросов летит одновременно.

        Таймауты вынесены в параметры, чтобы их можно было проверить
        в демо (день 1, пункт 7: «протестировать таймауты»).

        html_parser — день 2. Свой экземпляр нужен, если хочешь другие
        настройки разбора (например, оставлять только ссылки своего
        домена). Не передашь — создастся стандартный.
        """
        self.max_concurrent = max_concurrent
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

        # Сессию и семафор создаём лениво, при первом обращении.
        # Оба объекта привязываются к работающему event loop, а __init__
        # вызывается до asyncio.run() — то есть цикла ещё нет.
        self._session: aiohttp.ClientSession | None = None
        self._semaphore: asyncio.Semaphore | None = None

        # Счётчики для пунктов 6-7 задания: показать статус запросов
        # и подтвердить, что битые URL обработаны, а не уронили прогон.
        self.successful: int = 0
        self.failed: int = 0
        self.errors: dict[str, str] = {}

    # ---------- сессия ----------

    def _ensure_session(self) -> aiohttp.ClientSession:
        """Создаёт сессию и семафор при первом обращении."""
        if self._session is None or self._session.closed:
            # TCPConnector — это и есть connection pooling из пункта 3.
            # Соединения не закрываются после ответа, а складываются
            # в пул и переиспользуются: экономим TCP-хендшейк и TLS-
            # рукопожатие, а это десятки-сотни миллисекунд на запрос.
            connector = aiohttp.TCPConnector(limit=self.max_concurrent)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self._timeout,
            )
            logger.debug("Создана сессия, лимит соединений: %d", self.max_concurrent)

        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrent)

        return self._session

    async def close(self) -> None:
        """Закрывает сессию и освобождает соединения пула."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            logger.debug("Сессия закрыта")
        self._session = None

    # ---------- загрузка ----------

    async def fetch_url(self, url: str) -> str | None:
        """
        Загружает одну страницу.

        Возвращает HTML или None, если запрос не удался. Исключения
        наружу не пробрасываются — этого требует критерий успеха
        «ошибки обрабатываются без падения программы».
        """
        session = self._ensure_session()
        assert self._semaphore is not None

        async with self._semaphore:          # ждём свободный слот
            logger.info("Начинаю загрузку %s", url)

            try:
                async with session.get(url) as response:
                    # Превращает HTTP 4xx/5xx в ClientResponseError.
                    # Без этого вызова 404 вернётся как обычный успешный
                    # ответ с телом страницы ошибки внутри.
                    response.raise_for_status()

                    html = await response.text()

                    self.successful += 1
                    logger.info(
                        "Успешно %s — статус %d, %d символов",
                        url, response.status, len(html),
                    )
                    return html

            # ВАЖЕН ПОРЯДОК except. Иерархия исключений aiohttp:
            #
            #   Exception
            #   └── aiohttp.ClientError
            #       ├── ClientResponseError     ← HTTP 404, 500...
            #       └── ClientConnectionError
            #           └── ServerTimeoutError  ← он же asyncio.TimeoutError
            #
            # ClientResponseError — ПОТОМОК ClientError. Поставишь
            # except ClientError первым — он перехватит вообще всё,
            # и ветка с кодом ответа никогда не выполнится.
            # Всегда ловим от частного к общему.

            except aiohttp.ClientResponseError as e:
                self._record_error(url, f"ClientResponseError: HTTP {e.status}")

            except asyncio.TimeoutError:
                # В Python 3.11+ asyncio.TimeoutError — псевдоним
                # встроенного TimeoutError. Пишем через asyncio ради
                # совместимости со старыми версиями.
                self._record_error(url, "TimeoutError: превышен таймаут")

            except aiohttp.ClientError as e:
                # DNS не резолвится, обрыв соединения, отказ TLS
                self._record_error(url, f"{type(e).__name__}: {e}")

            except Exception as e:            # noqa: BLE001
                # Последний рубеж на случай неожиданного (битая кодировка,
                # некорректный URL). Логируем с трейсбеком, но прогон
                # не роняем — этого требует критерий успеха.
                logger.exception("Непредвиденная ошибка на %s", url)
                self._record_error(url, f"{type(e).__name__}: {e}")

            return None

    def _record_error(self, url: str, reason: str) -> None:
        """Пункт 5 задания: логировать ошибки с URL и типом ошибки."""
        self.failed += 1
        self.errors[url] = reason
        logger.warning("Ошибка на %s — %s", url, reason)

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        """
        Параллельно загружает список URL.

        Возвращает {url: html} только для успешных загрузок,
        неудачи остаются в self.errors.
        """
        self._ensure_session()

        # gather запускает все корутины конкурентно и ждёт их все.
        #
        # return_exceptions=True — страховка. По умолчанию первое же
        # исключение немедленно пробрасывается наружу из gather,
        # а остальные задачи остаются висеть незавершёнными. fetch_url
        # ловит свои ошибки сам, но если мы однажды напортачим в нём
        # самом — прогон всё равно доедет до конца.
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

        Возвращает словарь с полями url, title, text, links, metadata
        (плюс images, headings, tables, lists от парсера).

        Если загрузка не удалась, возвращается словарь той же формы,
        но с пустыми полями и заполненным ключом error. Так вызывающий
        код всегда получает dict и может работать единообразно,
        не проверяя результат на None.
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
                "error": self.errors.get(url, "не удалось загрузить"),
            }

        result = await self.html_parser.parse_html(html, url)
        result["error"] = None
        return result
