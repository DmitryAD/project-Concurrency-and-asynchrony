"""
День 5 (опционально). Circuit breaker.

Если домен подряд отвечает ошибками, он, скорее всего, лежит, и слать
ему запросы дальше бессмысленно. Автомат размыкается: запросы к домену
отклоняются сразу, без сети. Через recovery_timeout секунд пропускается
один пробный запрос: успех — всё как обычно, ошибка — снова блокировка.

Состояния:
    closed    — запросы идут
    open      — запросы сразу отклоняются
    half_open — идёт один пробный запрос, остальные ждут его исхода
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict

from crawler.errors import CircuitOpenError

logger = logging.getLogger("crawler.circuit")


class CircuitBreaker:
    """
        breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
        breaker.check(domain)            # бросит CircuitOpenError, если выбит
        ...
        breaker.record_success(domain)   # или record_failure(domain)
    """

    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0) -> None:
        """
        failure_threshold — сколько ошибок подряд размыкают автомат.
        recovery_timeout — через сколько секунд пробовать снова.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._failures: dict[str, int] = defaultdict(int)
        self._opened_at: dict[str, float] = {}
        self._probe_in_flight: set[str] = set()
        self.times_opened = 0
        self.rejected = 0

    def state(self, domain: str) -> str:
        if domain not in self._opened_at:
            return "closed"
        if time.monotonic() - self._opened_at[domain] < self.recovery_timeout:
            return "open"
        return "half_open"

    def check(self, domain: str, url: str = "") -> None:
        """Пропустить запрос или сразу отклонить его."""
        state = self.state(domain)
        if state == "closed":
            return
        if state == "half_open" and domain not in self._probe_in_flight:
            self._probe_in_flight.add(domain)       # пропускаем одного пробного
            logger.info("Автомат для %s: пробный запрос", domain)
            return
        self.rejected += 1
        raise CircuitOpenError(url, f"домен {domain} временно заблокирован")

    def record_success(self, domain: str) -> None:
        if domain in self._opened_at:
            logger.info("Автомат для %s снова включён", domain)
        self._failures[domain] = 0
        self._opened_at.pop(domain, None)
        self._probe_in_flight.discard(domain)

    def record_failure(self, domain: str) -> None:
        self._failures[domain] += 1
        was_probe = domain in self._probe_in_flight
        self._probe_in_flight.discard(domain)
        if was_probe or self._failures[domain] >= self.failure_threshold:
            if self.state(domain) != "open":
                self.times_opened += 1
            self._opened_at[domain] = time.monotonic()
            logger.warning("Автомат для %s выбит: %d ошибок подряд, пауза %.1f c",
                           domain, self._failures[domain], self.recovery_timeout)

    def get_stats(self) -> dict:
        return {
            "times_opened": self.times_opened,
            "rejected": self.rejected,
            "open_domains": [d for d in self._opened_at if self.state(d) == "open"],
        }