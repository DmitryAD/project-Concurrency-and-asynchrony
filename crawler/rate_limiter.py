"""
День 4. Ограничение скорости запросов.

Семафор из дня 3 отвечает на вопрос «сколько запросов идут ОДНОВРЕМЕННО».
RateLimiter отвечает на другой вопрос: «как ЧАСТО можно начинать новый».

    Семафор = 5:        5 запросов в полёте, но каждую долю секунды — новый
    2 запроса в секунду: новый запрос не раньше, чем через 0.5 c после прошлого

Для чужого сайта важно именно второе: нагрузку он ощущает по частоте.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections import defaultdict, deque

GLOBAL = "__global__"


class RateLimiter:
    """
        limiter = RateLimiter(requests_per_second=2.0)
        await limiter.acquire("site.com")   # вернётся, когда можно слать запрос

    Как устроено: для каждого домена хранится момент, раньше которого
    следующий запрос начинать нельзя. acquire() занимает ближайшее
    свободное «окно», сдвигает момент на интервал вперёд и спит до своего
    окна. Каждый вызов получает своё окно, поэтому одновременные вызовы
    выстраиваются в очередь, а не проскакивают разом.
    """

    def __init__(
        self,
        requests_per_second: float | None = 1.0,
        per_domain: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
        error_backoff: float = 0.0,
        max_backoff: float = 60.0,
    ) -> None:
        """
        requests_per_second — сколько запросов в секунду. None или 0 — без
            ограничения частоты (останутся только min_delay, Crawl-delay
            и backoff, если заданы).
        per_domain — True: у каждого домена свой лимит. False: один лимит
            на все запросы вместе.
        min_delay — минимальная пауза между запросами, в секундах.
        jitter — случайная добавка к паузе, от 0 до jitter секунд. Так
            запросы идут не строго по метроному.
        error_backoff — экспоненциальное замедление после ошибок сервера.
            0 — выключено. Иначе после ошибки к паузе добавляется это число,
            после следующей подряд — вдвое больше, и так до max_backoff.
            Первый же успешный ответ сбрасывает замедление.
        """
        self.base_interval = 1.0 / requests_per_second if requests_per_second else 0.0
        self.per_domain = per_domain
        self.min_delay = min_delay
        self.jitter = jitter
        self.error_backoff = error_backoff
        self.max_backoff = max_backoff

        self._next_allowed: dict[str, float] = defaultdict(float)
        self._locks: dict[str, asyncio.Lock] = {}
        self._crawl_delay: dict[str, float] = {}     # домен -> Crawl-delay
        self._backoff: dict[str, float] = defaultdict(float)

        # Для мониторинга (пункт 7)
        self._starts: dict[str, list[float]] = defaultdict(list)
        self._recent: deque[float] = deque()
        self.total_requests = 0
        self.total_wait = 0.0

    # ---------- настройка на ходу ----------

    def set_crawl_delay(self, domain: str, seconds: float) -> None:
        """Задержка из robots.txt для домена (пункт 4)."""
        self._crawl_delay[domain] = seconds

    def report_error(self, domain: str) -> None:
        """
        Сервер ответил ошибкой — замедляемся (если backoff включён).

        Окно для следующего запроса к этому моменту обычно уже
        забронировано: его рассчитали, когда отпускали текущий запрос,
        ещё до ответа. Поэтому мало запомнить новую паузу — нужно сразу
        отодвинуть ближайшее разрешённое время. Иначе замедление
        сработает на запрос позже, чем должно.
        """
        if not self.error_backoff:
            return
        key = self._key(domain)
        current = self._backoff[key]
        self._backoff[key] = min(self.max_backoff, current * 2 if current else self.error_backoff)
        self._next_allowed[key] = max(self._next_allowed[key],
                                      time.monotonic() + self._backoff[key])

    def report_success(self, domain: str) -> None:
        """Успешный ответ — замедление снимается."""
        self._backoff[self._key(domain)] = 0.0

    # ---------- главное ----------

    def _key(self, domain: str | None) -> str:
        return domain if (self.per_domain and domain) else GLOBAL

    def interval_for(self, domain: str | None) -> float:
        """
        Обычная пауза между запросами: самое строгое из ограничений.
        Замедление после ошибок сюда не входит — его сразу применяет
        report_error, сдвигая время следующего запроса.
        """
        return max(self.base_interval, self.min_delay, self._crawl_delay.get(domain or "", 0.0))

    async def acquire(self, domain: str | None = None) -> float:
        """
        Ждёт, пока можно отправить запрос к домену. Возвращает, сколько ждали.

        Если никаких ограничений нет, возвращается сразу.
        """
        key = self._key(domain)
        interval = self.interval_for(domain)

        # Быстрый путь: ограничений нет и никто не отодвинул время
        # следующего запроса (например, backoff после ошибки).
        if interval == 0 and not self.jitter and self._next_allowed[key] <= time.monotonic():
            self._record(key, 0.0)
            return 0.0

        if key not in self._locks:
            self._locks[key] = asyncio.Lock()

        # Замок держим только на время расчёта окна, а не на всё ожидание.
        # asyncio.Lock честный: кто раньше пришёл, тот раньше получит окно.
        async with self._locks[key]:
            now = time.monotonic()
            start = max(now, self._next_allowed[key])
            gap = interval + (random.uniform(0, self.jitter) if self.jitter else 0.0)
            self._next_allowed[key] = start + gap

        wait = start - now
        if wait > 0:
            await asyncio.sleep(wait)
        self._record(key, wait)
        return wait

    # ---------- мониторинг ----------

    def _record(self, key: str, wait: float) -> None:
        now = time.monotonic()
        self.total_requests += 1
        self.total_wait += wait
        self._starts[key].append(now)
        self._recent.append(now)

    def get_stats(self, window: float = 5.0) -> dict:
        """
        current_rps  — запросов в секунду за последние `window` секунд
        avg_wait     — сколько в среднем запрос ждал разрешения
        avg_interval — средняя пауза между соседними запросами к одному домену
        """
        now = time.monotonic()
        while self._recent and now - self._recent[0] > window:
            self._recent.popleft()

        gaps = [
            b - a
            for starts in self._starts.values()
            for a, b in zip(starts, starts[1:])
        ]
        return {
            "total_requests": self.total_requests,
            "current_rps": len(self._recent) / window,
            "avg_wait": self.total_wait / self.total_requests if self.total_requests else 0.0,
            "avg_interval": sum(gaps) / len(gaps) if gaps else 0.0,
            "backoff": {k: v for k, v in self._backoff.items() if v},
            "crawl_delay": dict(self._crawl_delay),
        }