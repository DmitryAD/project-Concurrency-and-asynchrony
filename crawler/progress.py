"""
День 7, пункт 7 — прогресс в реальном времени.

    [████████████░░░░░░░░░░░░░░░░░░]  40%  40/100 | 6.3 стр/с | осталось ~0:09 | в работе 5 | очередь 31 | ошибок 2

Монитор — ещё одна фоновая корутина рядом с воркерами (как
_report_progress из дня 3). Раз в interval секунд он берёт снимок
состояния у краулера (AsyncCrawler.snapshot) и перерисовывает строку.

Откуда процент, если заранее неизвестно, сколько на сайте страниц?
Знаменатель — меньшее из двух: лимит max_pages и «известная работа»
(сделано + в работе + ждёт в очереди). Пока обход идёт, сайт может
подкинуть новые ссылки, и знаменатель подрастёт — процент честно
покажет это, а не застрянет на 99%.

Оставшееся время = сколько осталось / текущая скорость. Это оценка:
если сайт начнёт отвечать медленнее, она изменится.
"""

from __future__ import annotations

import asyncio
import sys
from typing import Callable, TextIO


def format_duration(seconds: float | None) -> str:
    """75 -> '1:15', 3725 -> '1:02:05', None -> '?'."""
    if seconds is None or seconds != seconds or seconds == float("inf"):
        return "?"
    seconds = int(round(seconds))
    h, rest = divmod(seconds, 3600)
    m, s = divmod(rest, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


def estimate(snapshot: dict, max_pages: int) -> dict:
    """Процент, скорость и оставшееся время по снимку состояния."""
    done = snapshot["done"]
    known = done + snapshot["in_progress"] + snapshot["queued"]
    total = max(min(max_pages, known), done, 1)
    elapsed = snapshot["elapsed"]
    speed = done / elapsed if elapsed > 0 else 0.0
    remaining = total - done
    if remaining <= 0:
        eta = 0.0
    elif speed > 0:
        eta = remaining / speed
    else:
        eta = None                      # ещё ничего не готово — оценить нельзя
    return {"done": done, "total": total, "percent": 100.0 * done / total,
            "speed": speed, "eta": eta}


def format_progress(snapshot: dict, max_pages: int, width: int = 30) -> str:
    """Одна строка прогресса. Чистая функция — удобно проверять."""
    e = estimate(snapshot, max_pages)
    filled = int(width * e["done"] / e["total"])
    bar = "█" * filled + "░" * (width - filled)
    return (f"[{bar}] {e['percent']:3.0f}%  {e['done']}/{e['total']} | "
            f"{e['speed']:.1f} стр/с | осталось ~{format_duration(e['eta'])} | "
            f"в работе {snapshot['in_progress']} | очередь {snapshot['queued']} | "
            f"ошибок {snapshot['failed']}")


class ProgressMonitor:
    """
        monitor = ProgressMonitor(crawler.snapshot, max_pages=100)
        monitor.start()
        await crawler.crawl(...)
        await monitor.stop()

    source — функция без аргументов, возвращающая снимок (AsyncCrawler.snapshot).
    stream — куда писать; по умолчанию stderr, чтобы не смешиваться
        с полезным выводом программы в stdout.

    В настоящем терминале строка перерисовывается на месте (символ \\r
    возвращает курсор в начало строки). Если вывод перенаправлен в файл,
    \\r там только мешал бы — тогда пишем обычные строки, но реже.
    """

    def __init__(self, source: Callable[[], dict], max_pages: int,
                 interval: float = 0.5, stream: TextIO | None = None,
                 width: int = 30) -> None:
        self.source = source
        self.max_pages = max_pages
        self.interval = interval
        self.stream = stream or sys.stderr
        self.width = width
        self.inplace = bool(getattr(self.stream, "isatty", lambda: False)())
        self.lines_written = 0
        self._task: asyncio.Task | None = None
        self._last_len = 0

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Останавливает обновление и печатает финальную строку."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        self.render(final=True)

    async def _run(self) -> None:
        # Не на месте — реже: раз в 5 интервалов, чтобы не заваливать лог
        every = self.interval if self.inplace else self.interval * 5
        while True:
            await asyncio.sleep(every)
            self.render()

    def render(self, final: bool = False) -> None:
        line = format_progress(self.source(), self.max_pages, self.width)
        if self.inplace:
            pad = " " * max(self._last_len - len(line), 0)
            self.stream.write("\r" + line + pad + ("\n" if final else ""))
            self._last_len = len(line)
        else:
            self.stream.write(line + "\n")
        self.stream.flush()
        self.lines_written += 1