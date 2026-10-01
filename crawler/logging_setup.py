"""
День 7, пункт 6 — логирование в файл и консоль.

До сих пор каждое демо настраивало logging.basicConfig у себя.
Здесь — одна функция для всего приложения:

    setup_logging(level="DEBUG", console_level="WARNING",
                  log_file="output/crawler.log")

Что получается:

    консоль  — только важное (WARNING и выше), чтобы не мешать
               прогресс-бару;
    файл     — всё подробно (DEBUG и выше), с временем до миллисекунд.

Ротация: когда файл дорастает до max_bytes, он переименовывается
в crawler.log.1, старый .1 — в .2 и так далее; хранится backup_count
старых файлов, самый старый удаляется. Иначе лог долгого краулера
однажды съест весь диск.

Формат text — для чтения глазами, json — по записи на строку,
для машинной обработки (поиск, фильтры, загрузка в pandas).
"""

from __future__ import annotations

import json
import logging
import logging.handlers
import sys
from pathlib import Path

TEXT_FORMAT = "%(asctime)s.%(msecs)03d | %(levelname)-7s | %(name)-22s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# Метка на наших обработчиках: повторный вызов setup_logging
# заменяет их, не трогая чужие и не удваивая вывод.
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

    # Убрать то, что мы ставили раньше
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

    # Корневой логгер пропускает всё, что нужно хоть одному обработчику;
    # дальше каждый обработчик фильтрует по своему уровню.
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