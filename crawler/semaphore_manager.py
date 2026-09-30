"""
День 3. Управление конкурентностью.

Два уровня ограничений одновременно:
  - глобальный: сколько запросов летит всего;
  - по домену:  сколько запросов летит к ОДНОМУ сайту.

Зачем второй уровень: при max_concurrent=20 и обходе одного сайта
все 20 запросов полетят в один сервер. Для небольшого магазина это
уже ощутимая нагрузка. Лимит на домен держит её в разумных рамках,
а общий лимит при этом можно тратить на разные сайты.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from urllib.parse import urlparse


class SemaphoreManager:
    """
        manager = SemaphoreManager(max_concurrent=10, max_per_domain=3)
        async with manager.acquire(url):
            ...   # здесь запрос
    """

    def __init__(self, max_concurrent: int = 10, max_per_domain: int | None = None) -> None:
        """
        max_per_domain=None — отдельного лимита на домен нет, действует
        только общий max_concurrent. Так поведение дней 1-2 не меняется:
        код, который про домены ничего не знает, работает как раньше.
        """
        self.max_concurrent = max_concurrent
        self.max_per_domain = max_concurrent if max_per_domain is None else max_per_domain

        # Семафоры создаются лениво: глобальный — при первом запросе,
        # доменные — при первом запросе к каждому новому домену.
        self._global: asyncio.Semaphore | None = None
        self._domains: dict[str, asyncio.Semaphore] = {}

        # Отслеживание активных задач (пункт 2 задания)
        self._active: dict[str, int] = defaultdict(int)
        self._active_total = 0
        self.peak_total = 0
        self.peak_by_domain: dict[str, int] = defaultdict(int)

    @staticmethod
    def domain_of(url: str) -> str:
        return urlparse(url).netloc.lower()

    def _domain_semaphore(self, domain: str) -> asyncio.Semaphore:
        if domain not in self._domains:
            self._domains[domain] = asyncio.Semaphore(self.max_per_domain)
        return self._domains[domain]

    @asynccontextmanager
    async def acquire(self, url: str):
        """
        Занимает слот домена, потом глобальный слот.

        Порядок важен. Если сначала занять глобальный слот, а потом
        ждать доменный, задача будет простаивать с занятым глобальным
        слотом — и не пускать запросы к другим, свободным сайтам.
        Поэтому сначала ждём свой домен, и только потом берём общий слот.
        """
        if self._global is None:
            self._global = asyncio.Semaphore(self.max_concurrent)

        domain = self.domain_of(url)

        async with self._domain_semaphore(domain):
            async with self._global:
                self._active[domain] += 1
                self._active_total += 1
                self.peak_total = max(self.peak_total, self._active_total)
                self.peak_by_domain[domain] = max(
                    self.peak_by_domain[domain], self._active[domain]
                )
                try:
                    yield
                finally:
                    self._active[domain] -= 1
                    self._active_total -= 1

    def get_stats(self) -> dict:
        return {
            "active_total": self._active_total,
            "active_by_domain": {d: n for d, n in self._active.items() if n},
            "peak_total": self.peak_total,
            "peak_by_domain": dict(self.peak_by_domain),
        }