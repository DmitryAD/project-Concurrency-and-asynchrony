"""
День 3. Очередь URL с приоритетами.

Очередь — центр краулера. Сюда складываются стартовые адреса и все
найденные ссылки, отсюда рабочие задачи (воркеры) берут, что качать
дальше. Очередь же следит, чтобы один адрес не попал в работу дважды.
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

    Больше priority — раньше в работу. При равном приоритете —
    в порядке добавления.
    """

    def __init__(self) -> None:
        # asyncio.PriorityQueue всегда отдаёт МИНИМАЛЬНЫЙ элемент.
        # Кладём кортеж (-priority, номер, url): минус переворачивает
        # порядок, а порядковый номер решает ничьи по приоритету
        # и не даёт Python сравнивать сами URL.
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
        Приводит URL к единому виду, чтобы дубликаты распознавались.

        https://Site.com и https://site.com/ — один и тот же адрес,
        но как строки они разные. Домен в нижний регистр, пустой
        путь превращаем в "/".
        """
        parts = urlparse(url)
        path = parts.path or "/"
        return urlunparse((parts.scheme, parts.netloc.lower(), path,
                           parts.params, parts.query, ""))

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        """
        Добавляет URL в очередь.

        Возвращает True, если URL добавлен, и False, если он уже был
        (в очереди, в работе или обработан). Именно здесь отсекаются
        дубликаты: второй раз один адрес в очередь не попадёт никогда.

        depth — глубина от стартовой страницы (пункт 5 задания).
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
        Выдаёт следующий URL с наибольшим приоритетом.

        Если очередь пуста, ЖДЁТ, а не возвращает None сразу. Это
        важно: очередь может опустеть на мгновение, пока другие воркеры
        ещё качают страницы и вот-вот добавят новые ссылки.

        None возвращается только после close() — сигнал «работы больше
        не будет, завершайся».
        """
        _, _, url = await self._queue.get()
        return url

    def get_depth(self, url: str) -> int:
        return self._depths.get(self.normalize(url), 0)

    # ---------- отметки о результате ----------
    #
    # На каждый полученный через get_next URL нужно вызвать РОВНО ОДИН
    # из трёх методов ниже. Внутри они вызывают task_done() — так очередь
    # понимает, что задача закрыта. Когда закрыты все, join() отпускает
    # ожидание и краулер завершается.

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
        Будит всех воркеров сигналом «конец работы».

        Кладёт в очередь по одной метке None на каждого воркера.
        Приоритет — бесконечно низкий, так что метки выйдут последними.
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