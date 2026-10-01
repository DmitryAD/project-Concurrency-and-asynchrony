"""
День 3. Управление конкурентностью.

Два уровня ограничений:
  - глобальный — сколько запросов идёт всего;
  - по домену  — сколько идёт к одному сайту.

Без второго уровня при max_concurrent=20 и одном сайте все 20 запросов
шли бы в один сервер. Лимит на домен держит нагрузку на сайт, а общий
лимит можно делить между разными сайтами.
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
        max_per_domain=None — отдельного лимита на домен нет, работает только
        max_concurrent. Так дни 1-2 ведут себя как раньше.
        """
        self.max_concurrent = max_concurrent
        self.max_per_domain = max_concurrent if max_per_domain is None else max_per_domain

        # семафоры создаются лениво, при первом запросе (к домену)
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
        Занимает слот домена, потом глобальный.

        Если сначала взять глобальный слот и потом ждать доменный, задача
        простаивает с занятым глобальным слотом и не пускает запросы к другим
        сайтам. Поэтому сначала свой домен, потом общий слот.
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