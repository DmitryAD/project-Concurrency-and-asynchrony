"""
День 5. Классификация ошибок.

Главный вопрос при любой ошибке: есть ли смысл попробовать ещё раз?

    TransientError  — временная: сервер перегружен, таймаут, 503, 429.
                      Через пару секунд может пройти — повторяем.
    PermanentError  — постоянная: 404, 403, 401. Страницы нет или
                      нас не пускают — повтор даст то же самое.
    NetworkError    — сеть: соединение отклонено, DNS не нашёл домен.
                      Иногда проходит со второго раза, повторяем осторожно.
    ParseError      — не удалось разобрать ответ. Повтор бессмысленен:
                      тот же HTML разберётся с той же ошибкой.

Иерархия:

    CrawlerError
    ├── TransientError
    │   └── RateLimitedError   (429, может нести Retry-After)
    ├── PermanentError
    ├── NetworkError
    ├── ParseError
    └── CircuitOpenError       (домен временно заблокирован, см. circuit_breaker)
"""

from __future__ import annotations

import asyncio

import aiohttp


class CrawlerError(Exception):
    """
    Общий предок всех ошибок краулера.

    reason   — текст для лога и отчёта, например "ClientResponseError: HTTP 404"
    status   — HTTP-код, если ошибка пришла от сервера
    attempts — сколько попыток было сделано к моменту, когда сдались
    """

    def __init__(self, url: str, reason: str, status: int | None = None) -> None:
        super().__init__(reason)
        self.url = url
        self.reason = reason
        self.status = status
        self.attempts = 1


class TransientError(CrawlerError):
    """Временная ошибка — имеет смысл повторить."""


class RateLimitedError(TransientError):
    """429 Too Many Requests. retry_after — сколько секунд просит подождать сервер."""

    def __init__(self, url: str, reason: str, status: int | None = 429,
                 retry_after: float | None = None) -> None:
        super().__init__(url, reason, status)
        self.retry_after = retry_after


class PermanentError(CrawlerError):
    """Постоянная ошибка — повторять бесполезно."""


class NetworkError(CrawlerError):
    """Сетевая ошибка: не удалось соединиться или найти домен."""


class ParseError(CrawlerError):
    """Ответ получен, но разобрать его не удалось."""


class CircuitOpenError(CrawlerError):
    """Домен временно заблокирован автоматом защиты — запрос даже не отправлялся."""


# Коды, при которых сервер сам говорит «попробуй позже»
TRANSIENT_STATUSES = {408, 425, 429, 500, 502, 503, 504}


def parse_retry_after(value: str | None) -> float | None:
    """
    Заголовок Retry-After: число секунд. Бывает ещё дата, но её
    почти не используют — такой вариант просто игнорируем.
    """
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None


def classify_status(url: str, status: int, reason: str,
                    retry_after: str | None = None) -> CrawlerError:
    """HTTP-код ответа -> тип ошибки."""
    if status == 429:
        return RateLimitedError(url, reason, status, parse_retry_after(retry_after))
    if status in TRANSIENT_STATUSES or status >= 500:
        return TransientError(url, reason, status)
    return PermanentError(url, reason, status)


def classify_exception(exc: BaseException, url: str = "") -> CrawlerError:
    """
    Любое исключение -> тип ошибки краулера.

    Нужно, чтобы RetryStrategy понимала и «сырые» ошибки aiohttp,
    если повторять через неё не краулер, а любую другую функцию.
    """
    if isinstance(exc, CrawlerError):
        return exc
    if isinstance(exc, aiohttp.ClientResponseError):
        headers = getattr(exc, "headers", None) or {}
        return classify_status(url, exc.status, f"ClientResponseError: HTTP {exc.status}",
                               headers.get("Retry-After"))
    # Порядок важен: ServerTimeoutError — одновременно и сетевая ошибка,
    # и таймаут. Таймаут проверяем раньше.
    if isinstance(exc, asyncio.TimeoutError):
        return TransientError(url, "TimeoutError: превышен таймаут")
    if isinstance(exc, aiohttp.ClientError):
        return NetworkError(url, f"{type(exc).__name__}: {exc}")
    if isinstance(exc, UnicodeDecodeError):
        return ParseError(url, f"{type(exc).__name__}: {exc}")
    return CrawlerError(url, f"{type(exc).__name__}: {exc}")