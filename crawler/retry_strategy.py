"""
День 5. Автоматические повторы с экспоненциальным backoff.

Экспоненциальный backoff: пауза перед каждым следующим повтором
растёт в backoff_factor раз.

    base_delay=0.5, backoff_factor=2:
    попытка 1 — ошибка, ждём 0.5 c
    попытка 2 — ошибка, ждём 1.0 c
    попытка 3 — ошибка, ждём 2.0 c
    попытка 4 — последняя

Почему растёт, а не одинаковая: если сервер перегружен, частые
повторы только добивают его. Растущая пауза даёт ему время прийти
в себя, а нам — шанс на успех.

Чем отличается от error_backoff из дня 4: там мы замедляем ВСЕ
следующие запросы к домену. Здесь — повторяем ОДИН конкретный
неудачный запрос.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from typing import Any, Awaitable, Callable

from crawler.errors import (
    CrawlerError,
    NetworkError,
    PermanentError,
    RateLimitedError,
    TransientError,
    classify_exception,
)

logger = logging.getLogger("crawler.retry")


class RetryStrategy:
    """
        strategy = RetryStrategy(max_retries=3, backoff_factor=2.0,
                                 retry_on=[TransientError, NetworkError])
        html = await strategy.execute_with_retry(crawler.fetch_once, url)
    """

    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
        retry_on: list[type[CrawlerError]] | None = None,
        base_delay: float = 0.5,
        max_delay: float = 30.0,
        base_delay_by_type: dict[type[CrawlerError], float] | None = None,
        max_retries_by_type: dict[type[CrawlerError], int] | None = None,
        max_retries_by_status: dict[int, int] | None = None,
        timeout_growth: float = 1.5,
    ) -> None:
        """
        max_retries — сколько повторов максимум (попыток = повторы + 1).
        backoff_factor — во сколько раз растёт пауза с каждым повтором.
        retry_on — какие типы ошибок повторять. По умолчанию временные
            и сетевые. Постоянные (404, 403) не повторяются.

        Тонкая настройка (пункт 3 задания):
        base_delay — пауза перед первым повтором.
        max_delay — потолок паузы, чтобы не ждать часами.
        base_delay_by_type — своя первая пауза для типа ошибки.
            По умолчанию: 429 — 2 c, сеть — 1 c, остальное — base_delay.
        max_retries_by_type — свой лимит повторов для типа.
            По умолчанию сетевые ошибки — не больше 2 повторов.
        max_retries_by_status — свой лимит для HTTP-кода.
            По умолчанию 500 — один повтор (пункт 5 задания).
        timeout_growth — во сколько раз увеличивать таймаут с каждой
            попыткой (пункт 6). Применяется краулером в fetch_url.
        """
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = tuple(retry_on or (TransientError, NetworkError))
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.base_delay_by_type = (
            base_delay_by_type if base_delay_by_type is not None
            else {RateLimitedError: 2.0, NetworkError: 1.0}
        )
        self.max_retries_by_type = (
            max_retries_by_type if max_retries_by_type is not None
            else {NetworkError: 2}
        )
        self.max_retries_by_status = (
            max_retries_by_status if max_retries_by_status is not None
            else {500: 1}
        )
        self.timeout_growth = timeout_growth

        # Статистика (пункт 9)
        self.errors_by_type: Counter = Counter()   # каждая неудачная попытка
        self.total_retries = 0
        self.total_retry_wait = 0.0
        self.successful_retries = 0                # URL, которые прошли не с первого раза
        self.failed_after_retries = 0              # URL, которые так и не прошли
        self.permanent_error_urls: list[str] = []

    # ---------- правила ----------

    @staticmethod
    def _lookup(table: dict[type, Any], err: CrawlerError) -> Any:
        """Значение для самого точного типа ошибки (по цепочке наследования)."""
        for cls in type(err).__mro__:
            if cls in table:
                return table[cls]
        return None

    def should_retry(self, err: CrawlerError) -> bool:
        return isinstance(err, self.retry_on)

    def max_retries_for(self, err: CrawlerError) -> int:
        limits = [self.max_retries]
        by_type = self._lookup(self.max_retries_by_type, err)
        if by_type is not None:
            limits.append(by_type)
        if err.status is not None and err.status in self.max_retries_by_status:
            limits.append(self.max_retries_by_status[err.status])
        return min(limits)

    def delay_for(self, err: CrawlerError, retry_number: int) -> float:
        """Пауза перед повтором номер retry_number (1, 2, 3...)."""
        base = self._lookup(self.base_delay_by_type, err)
        base = self.base_delay if base is None else base
        delay = base * self.backoff_factor ** (retry_number - 1)
        # 429 с Retry-After: сервер сам сказал, сколько ждать — не меньше этого
        if isinstance(err, RateLimitedError) and err.retry_after is not None:
            delay = max(delay, err.retry_after)
        return min(delay, self.max_delay)

    # ---------- главное ----------

    async def execute_with_retry(
        self,
        coro: Callable[..., Awaitable[Any]],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Вызывает coro(*args, **kwargs) и при ошибке повторяет по правилам.

        coro — асинхронная функция, которая при неудаче БРОСАЕТ исключение.
        Возвращает её результат или бросает последнюю ошибку, приведённую
        к одному из типов CrawlerError.
        """
        url = kwargs.get("url") or next((a for a in args if isinstance(a, str)), "")
        attempt = 0

        while True:
            attempt += 1
            try:
                result = await coro(*args, **kwargs)
            except Exception as raw:  # noqa: BLE001
                err = classify_exception(raw, url)
                err.attempts = attempt
                self.errors_by_type[type(err).__name__] += 1

                retries_done = attempt - 1
                limit = self.max_retries_for(err)

                if not self.should_retry(err) or retries_done >= limit:
                    self._give_up(err, url, attempt, limit)
                    if err is raw:
                        raise
                    raise err from raw

                delay = self.delay_for(err, retries_done + 1)
                self.total_retries += 1
                self.total_retry_wait += delay
                logger.warning(
                    "%s на %s (попытка %d из %d): %s — повтор через %.2f c",
                    type(err).__name__, url, attempt, limit + 1, err.reason, delay,
                )
                await asyncio.sleep(delay)
                continue

            if attempt > 1:
                self.successful_retries += 1
                logger.info("Успех на %s с попытки %d", url, attempt)
            return result

    def _give_up(self, err: CrawlerError, url: str, attempt: int, limit: int) -> None:
        if isinstance(err, PermanentError):
            self.permanent_error_urls.append(url)
            logger.warning("%s на %s: %s — постоянная ошибка, не повторяем",
                           type(err).__name__, url, err.reason)
        elif not self.should_retry(err):
            logger.warning("%s на %s: %s — этот тип не повторяем",
                           type(err).__name__, url, err.reason)
        else:
            self.failed_after_retries += 1
            logger.error("%s на %s: %s — сдаёмся после %d попыток",
                         type(err).__name__, url, err.reason, attempt)

    # ---------- статистика ----------

    def get_stats(self) -> dict:
        return {
            "errors_by_type": dict(self.errors_by_type),
            "total_retries": self.total_retries,
            "successful_retries": self.successful_retries,
            "failed_after_retries": self.failed_after_retries,
            "avg_retry_delay": (self.total_retry_wait / self.total_retries
                                if self.total_retries else 0.0),
            "permanent_error_urls": list(self.permanent_error_urls),
        }