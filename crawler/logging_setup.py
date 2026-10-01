"""
День 7, пункт 6 — логирование в файл и консоль.

Одна функция настройки для всего приложения вместо basicConfig в каждом демо:

    setup_logging(level="DEBUG", console_level="WARNING",
                  log_file="output/crawler.log")

    консоль — только WARNING и выше, чтобы не мешать прогресс-бару;
    файл    — всё с DEBUG, время до миллисекунд.

Ротация: дорос до max_bytes — файл становится crawler.log.1, старый .1 —
.2 и т. д., хранится backup_count старых, самый старый удаляется.

Формат text — для чтения глазами, json — запись на строку для разбора
программой (поиск, фильтры, pandas).
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import sys
from pathlib import Path

TEXT_FORMAT = "%(asctime)s.%(msecs)03d | %(levelname)-7s | %(name)-22s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# метка своих обработчиков: повторный вызов заменяет их, не трогая чужие
_MARK = "_crawler_handler"


class JsonFormatter(logging.Formatter):
    """Одна запись лога — одна строка JSON."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "time": self.formatTime(record, DATE_FORMAT) + f".{int(record.msecs):03d}",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


def setup_logging(
    level: str = "INFO",
    console_level: str | None = None,
    log_file: str | Path | None = None,
    max_bytes: int = 1_000_000,
    backup_count: int = 3,
    fmt: str = "text",
    quiet_libraries: bool = True,
) -> logging.Logger:
    """
    level — с какого уровня писать в файл (DEBUG, INFO, WARNING, ERROR).
    console_level — с какого уровня показывать в консоли. None — как level.
    log_file — путь к файлу. None — только консоль.
    max_bytes, backup_count — ротация (см. описание модуля).
    fmt — "text" или "json" для файла. В консоли всегда текст.
    quiet_libraries — приглушить болтливые сторонние библиотеки.

    Возвращает корневой логгер.
    """
    level = level.upper()
    console_level = (console_level or level).upper()
    root = logging.getLogger()

    # снять обработчики, поставленные раньше
    for handler in list(root.handlers):
        if getattr(handler, _MARK, False):
            root.removeHandler(handler)
            handler.close()

    console = logging.StreamHandler(sys.stderr)
    console.setLevel(console_level)
    console.setFormatter(logging.Formatter(TEXT_FORMAT, DATE_FORMAT))
    setattr(console, _MARK, True)
    root.addHandler(console)

    lowest = logging.getLevelName(console_level)
    if log_file is not None:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(
            JsonFormatter() if fmt == "json" else logging.Formatter(TEXT_FORMAT, DATE_FORMAT)
        )
        setattr(file_handler, _MARK, True)
        root.addHandler(file_handler)
        lowest = min(lowest, logging.getLevelName(level))

    # корень пропускает всё, что нужно хоть одному обработчику, дальше фильтр по их уровням
    root.setLevel(lowest)

    if quiet_libraries:
        for name in ("aiohttp", "asyncio", "aiosqlite"):
            logging.getLogger(name).setLevel(logging.WARNING)
    return root


def remove_logging_handlers() -> None:
    """Снимает обработчики, поставленные setup_logging, и закрывает файл лога."""
    root = logging.getLogger()
    for handler in list(root.handlers):
        if getattr(handler, _MARK, False):
            root.removeHandler(handler)
            handler.close()