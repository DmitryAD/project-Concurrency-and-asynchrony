"""
День 3. Очередь URL с приоритетами.

Сюда попадают стартовые адреса и все найденные ссылки, отсюда воркеры
берут следующий URL. Очередь же отсекает дубликаты.
"""

from __future__ import annotations

import asyncio
import itertools
from urllib.parse import urlparse, urlunparse


class CrawlerQueue:
    """
    Очередь URL с приоритетами.

        queue = CrawlerQueue()
        queue.add_url("https://site.com", priority=10)
        url = await queue.get_next()
        ...
        queue.mark_processed(url)

    Больший priority выходит раньше, при равном — в порядке добавления.

    Внутри asyncio.PriorityQueue, которая отдаёт минимальный элемент,
    поэтому храню (-priority, номер, url). Номер решает ничьи и не даёт
    сравнивать сами URL.

    На каждый URL из get_next нужно вызвать ровно один из mark_processed,
    mark_failed, mark_skipped: они вызывают task_done(), и join() отпускает,
    только когда отмечены все.
    """

    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._counter = itertools.count()

        self._seen: set[str] = set()         # всё, что когда-либо добавляли
        self._depths: dict[str, int] = {}    # глубина каждого URL

        self._processed: set[str] = set()
        self._failed: dict[str, str] = {}
        self._skipped: set[str] = set()
        self._duplicates_rejected = 0
        self._closed = False

    # ---------- добавление ----------

    @staticmethod
    def normalize(url: str) -> str:
        """
        Приводит URL к единому виду для поиска дублей.

        https://Site.com и https://site.com/ — один адрес: домен в нижний
        регистр, пустой путь -> "/".
        """
        parts = urlparse(url)
        path = parts.path or "/"
        return urlunparse((parts.scheme, parts.netloc.lower(), path,
                           parts.params, parts.query, ""))

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        """
        Добавляет URL в очередь.

        True — добавлен, False — уже был (в очереди, в работе или обработан).
        depth — глубина от стартовой страницы (пункт 5).
        """
        if self._closed:
            return False

        url = self.normalize(url)
        if url in self._seen:
            self._duplicates_rejected += 1
            return False

        self._seen.add(url)
        self._depths[url] = depth
        self._queue.put_nowait((-priority, next(self._counter), url))
        return True

    # ---------- выдача ----------

    async def get_next(self) -> str | None:
        """
        Следующий URL с наибольшим приоритетом.

        На пустой очереди ждёт, а не возвращает None: она может опустеть
        на миг, пока другие воркеры ещё добавляют ссылки. None приходит
        только после close() — сигнал воркеру завершаться.
        """
        _, _, url = await self._queue.get()
        return url

    def get_depth(self, url: str) -> int:
        return self._depths.get(self.normalize(url), 0)

    # ---------- отметки о результате ----------

    def mark_processed(self, url: str) -> None:
        self._processed.add(self.normalize(url))
        self._queue.task_done()

    def mark_failed(self, url: str, error: str) -> None:
        self._failed[self.normalize(url)] = error
        self._queue.task_done()

    def mark_skipped(self, url: str) -> None:
        """URL взят, но не обработан — например, лимит страниц исчерпан."""
        self._skipped.add(self.normalize(url))
        self._queue.task_done()

    # ---------- завершение ----------

    async def join(self) -> None:
        """Ждёт, пока все добавленные URL будут отмечены."""
        await self._queue.join()

    def close(self, workers: int) -> None:
        """
        Будит воркеров сигналом завершения: по одной метке None на каждого
        с бесконечно низким приоритетом, чтобы они вышли последними.
        """
        self._closed = True
        for _ in range(workers):
            self._queue.put_nowait((float("inf"), next(self._counter), None))

    # ---------- статистика ----------

    def qsize(self) -> int:
        return self._queue.qsize()

    def get_stats(self) -> dict:
        return {
            "queued": self._queue.qsize() if not self._closed else 0,
            "processed": len(self._processed),
            "failed": len(self._failed),
            "skipped": len(self._skipped),
            "total_seen": len(self._seen),
            "duplicates_rejected": self._duplicates_rejected,
        }