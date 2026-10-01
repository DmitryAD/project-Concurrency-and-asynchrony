"""
День 7, пункты 2 и 3 — расширенная статистика и её экспорт.

CrawlerStats копит сведения о каждой обработанной странице:
когда закончилась, каким кодом ответил сервер, с какого домена,
какой была ошибка. Из них в get_stats() считаются итоги:

    total_pages, successful, failed     — сколько и с каким исходом
    pages_per_second                    — средняя скорость
    status_codes                        — распределение по кодам ответа
    top_domains                         — домены с наибольшим числом страниц
    duration_seconds                    — время работы
    timeline                            — сколько страниц готово в каждую секунду

Экспорт: export_to_json — всё то же самое файлом JSON,
export_to_html_report — страница с графиками и таблицами (crawler/report.py).

Записи приходят из AsyncCrawler через крючок on_page_done. Сам класс
о краулере ничего не знает — его можно наполнять и вручную, как
в проверках.
"""

from __future__ import annotations

import json
import math
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

NO_RESPONSE = "нет ответа"      # «код» для запросов, не дошедших до сервера


class CrawlerStats:
    """
        stats = CrawlerStats()
        stats.start()
        ... stats.record_page(data) после каждой страницы ...
        stats.finish()
        print(stats.get_stats()["pages_per_second"])
    """

    def __init__(self, top_n: int = 10, clock=time.monotonic) -> None:
        """
        top_n — сколько доменов показывать в топе.
        clock — источник времени. Подменяется в проверках, чтобы
            скорость считалась на точно известных числах.
        """
        self.top_n = top_n
        self._clock = clock
        self.started: float | None = None
        self.finished: float | None = None
        self.started_at: datetime | None = None     # календарное время — для отчёта
        self.finished_at: datetime | None = None

        self.successful = 0
        self.failed = 0
        self.blocked_by_robots = 0
        self.status_codes: Counter = Counter()
        self.domains: Counter = Counter()
        self.errors_by_type: Counter = Counter()
        self.depths: Counter = Counter()
        self.bytes_total = 0
        self.completion_times: list[float] = []     # секунды от старта
        self.failure_times: list[float] = []

    # ---------- наполнение ----------

    def start(self) -> None:
        self.started = self._clock()
        self.started_at = datetime.now()
        self.finished = self.finished_at = None

    def finish(self) -> None:
        self.finished = self._clock()
        self.finished_at = datetime.now()

    def record_page(self, data: dict, error_type: str | None = None) -> None:
        """
        data — словарь страницы из fetch_and_parse: нужны url,
            status_code, error, text, depth (что есть).
        error_type — класс ошибки (TransientError, PermanentError...),
            если страница не удалась. Без него берётся «Ошибка».
        """
        if self.started is None:
            self.start()
        at = self._clock() - self.started

        status = data.get("status_code")
        self.status_codes[str(status) if status is not None else NO_RESPONSE] += 1
        self.domains[urlparse(data.get("url", "")).netloc.lower() or "?"] += 1
        if data.get("depth") is not None:
            self.depths[int(data["depth"])] += 1

        if data.get("error"):
            self.failed += 1
            self.errors_by_type[error_type or "Ошибка"] += 1
            self.failure_times.append(at)
        else:
            self.successful += 1
            self.bytes_total += len(data.get("text") or "")
        self.completion_times.append(at)

    # ---------- итоги ----------

    @property
    def total_pages(self) -> int:
        return self.successful + self.failed

    @property
    def duration(self) -> float:
        if self.started is None:
            return 0.0
        end = self.finished if self.finished is not None else self._clock()
        return max(end - self.started, 0.0)

    def get_stats(self) -> dict:
        duration = self.duration
        total = self.total_pages
        return {
            "total_pages": total,
            "successful": self.successful,
            "failed": self.failed,
            "blocked_by_robots": self.blocked_by_robots,
            "success_rate": round(100 * self.successful / total, 1) if total else 0.0,
            "duration_seconds": round(duration, 3),
            "pages_per_second": round(total / duration, 2) if duration > 0 else 0.0,
            "avg_text_chars": round(self.bytes_total / self.successful) if self.successful else 0,
            "status_codes": dict(sorted(self.status_codes.items(), key=_status_order)),
            "top_domains": [
                {"domain": d, "pages": n} for d, n in self.domains.most_common(self.top_n)
            ],
            "domains_total": len(self.domains),
            "errors_by_type": dict(self.errors_by_type.most_common()),
            "depth_distribution": {str(d): n for d, n in sorted(self.depths.items())},
            "timeline": self.timeline(),
            "started_at": self.started_at.isoformat(timespec="seconds") if self.started_at else None,
            "finished_at": self.finished_at.isoformat(timespec="seconds") if self.finished_at else None,
        }

    def timeline(self, max_bins: int = 60) -> dict:
        """
        Сколько страниц завершилось в каждом отрезке времени.
        Отрезок — 1 секунда; для долгих обходов шире, чтобы
        столбиков было не больше max_bins.
        """
        duration = self.duration
        if not self.completion_times or duration <= 0:
            return {"bin_seconds": 1, "pages": [], "failed": []}
        bin_seconds = max(1, math.ceil(duration / max_bins))
        n_bins = max(1, math.ceil(duration / bin_seconds))

        def bins(times: list[float]) -> list[int]:
            counts = [0] * n_bins
            for t in times:
                counts[min(int(t // bin_seconds), n_bins - 1)] += 1
            return counts

        return {"bin_seconds": bin_seconds,
                "pages": bins(self.completion_times),
                "failed": bins(self.failure_times)}

    # ---------- экспорт ----------

    def export_to_json(self, filename: str | Path, extra: dict | None = None) -> Path:
        """Статистика в JSON. extra — что добавить (настройки, детали ошибок)."""
        data = self.get_stats()
        if extra:
            data.update(extra)
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str),
                        encoding="utf-8")
        return path

    def export_to_html_report(self, filename: str | Path, title: str = "Отчёт краулера",
                              extra: dict | None = None) -> Path:
        """HTML-страница с графиками и таблицами. Открывается в любом браузере без интернета."""
        from crawler.report import render_html_report

        data = self.get_stats()
        if extra:
            data.update(extra)
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_html_report(data, title=title), encoding="utf-8")
        return path


def _status_order(item: tuple[str, int]) -> tuple:
    """Коды по возрастанию, «нет ответа» — в конце."""
    code = item[0]
    return (1, 0) if not code.isdigit() else (0, int(code))