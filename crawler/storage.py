"""
День 6. Сохранение данных: JSON, CSV, SQLite.

Все хранилища работают одинаково:

    save(record) ──► буфер ──(набралось batch_size)──► запись на диск пачкой
                                          │
                            ошибка записи? повтор, потом лог и работа дальше

Пачки — потому что запись на диск и коммит в базе дорогие: 1000 записей
по одной — 1000 обращений к диску, пачкой — одно.

save() не бросает исключений: из-за секундной недоступности диска
нельзя терять весь обход. Ошибка записи логируется и считается
в статистике.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

# без aiosqlite остальное работает, ошибка только при создании SQLiteStorage
try:
    import aiofiles
except ImportError:  # pragma: no cover
    aiofiles = None
try:
    import aiosqlite
except ImportError:  # pragma: no cover
    aiosqlite = None

logger = logging.getLogger("crawler.storage")


def _require(module, name: str) -> None:
    if module is None:
        raise ImportError(f"Для этого хранилища нужна библиотека {name}: pip install {name}")

# Пункт 6: стандартный набор полей одной записи
STANDARD_FIELDS = (
    "url", "title", "text", "links", "metadata",
    "crawled_at", "status_code", "content_type",
)


def to_record(page: dict) -> dict:
    """
    Приводит результат fetch_and_parse к стандартной записи (пункт 6).

    Лишние поля (картинки, таблицы и т. д.) отбрасываются, недостающие
    заполняются пустыми. crawled_at — момент сохранения в UTC, если не задан.
    """
    crawled_at = page.get("crawled_at") or datetime.now(timezone.utc)
    return {
        "url": page.get("url", ""),
        "title": page.get("title", "") or "",
        "text": page.get("text", "") or "",
        "links": list(page.get("links") or []),
        "metadata": dict(page.get("metadata") or {}),
        "crawled_at": crawled_at,
        "status_code": page.get("status_code"),
        "content_type": page.get("content_type"),
    }


def _json_default(value):
    """Чем json.dumps заменяет то, что не умеет сам: дату — строкой ISO."""
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Не умею сохранить {type(value).__name__}")


class DataStorage(ABC):
    """
    Базовый класс хранилищ.

    Наследник реализует только:
        _write_batch(records) — записать пачку
        _close()              — освободить файл или соединение
    Буфер, пачки, повторы и статистика общие и живут здесь.
    """

    def __init__(self, batch_size: int = 50, write_retries: int = 2,
                 retry_delay: float = 0.2) -> None:
        """
        batch_size — сколько записей копить перед записью на диск.
            1 — писать каждую запись сразу.
        write_retries — сколько раз повторить неудачную запись пачки.
        retry_delay — пауза перед повтором, удваивается с каждым разом.
        """
        self.batch_size = max(1, batch_size)
        self.write_retries = write_retries
        self.retry_delay = retry_delay

        self._buffer: list[dict] = []
        self._lock = asyncio.Lock()      # две пачки не пишутся одновременно
        self._closed = False

        self.saved = 0            # записей успешно записано
        self.failed = 0           # записей потеряно после всех повторов
        self.batches = 0          # сколько пачек записано
        self.write_errors = 0     # сколько раз запись падала (включая повторы)

    async def save(self, data: dict) -> None:
        """Добавляет запись. На диск она попадёт с ближайшей пачкой."""
        self._buffer.append(to_record(data))
        if len(self._buffer) >= self.batch_size:
            await self.flush()

    async def flush(self) -> None:
        """
        Записывает всё, что накопилось в буфере.

        Под замком, чтобы две корутины не писали в один файл. Буфер забирается
        целиком и сразу очищается: новые записи уйдут следующей пачкой.
        """
        async with self._lock:
            if not self._buffer:
                return
            batch, self._buffer = self._buffer, []
            await self._write_with_retries(batch)

    async def _write_with_retries(self, batch: list[dict]) -> None:
        delay = self.retry_delay
        for attempt in range(1, self.write_retries + 2):
            try:
                await self._write_batch(batch)
            except Exception as e:  # noqa: BLE001
                self.write_errors += 1
                if attempt > self.write_retries:
                    self.failed += len(batch)
                    logger.error("%s: не удалось записать %d записей после %d попыток: %s: %s",
                                 type(self).__name__, len(batch), attempt, type(e).__name__, e)
                    return
                logger.warning("%s: ошибка записи (попытка %d): %s: %s — повтор через %.2f c",
                               type(self).__name__, attempt, type(e).__name__, e, delay)
                await asyncio.sleep(delay)
                delay *= 2
            else:
                self.saved += len(batch)
                self.batches += 1
                return

    async def close(self) -> None:
        """Дописывает буфер и закрывает файл или соединение."""
        if self._closed:
            return
        await self.flush()
        try:
            await self._close()
        except Exception as e:  # noqa: BLE001
            logger.error("%s: ошибка при закрытии: %s", type(self).__name__, e)
        self._closed = True

    def get_stats(self) -> dict:
        return {
            "storage": type(self).__name__,
            "saved": self.saved,
            "failed": self.failed,
            "batches": self.batches,
            "write_errors": self.write_errors,
            "buffered": len(self._buffer),
        }

    @abstractmethod
    async def _write_batch(self, records: list[dict]) -> None: ...

    async def _close(self) -> None:  # noqa: B027 — не обязательно переопределять
        pass


# ---------- JSON ----------

class JSONStorage(DataStorage):
    """
    Два режима.

    pretty=False (по умолчанию) — JSON Lines, одна запись на строку.
        Дописывается в конец, читается построчно, при падении программы
        записанное остаётся целым. Подходит для больших объёмов.

    pretty=True — JSON-массив с отступами, удобно читать глазами.
        Пишется тоже по частям, но валидным становится только после
        close(), когда дописывается закрывающая скобка.
    """

    def __init__(self, path: str | Path, pretty: bool = False, **kwargs) -> None:
        _require(aiofiles, "aiofiles")
        super().__init__(**kwargs)
        self.path = Path(path)
        self.pretty = pretty
        self._file = None
        self._written_any = False

    async def _open(self) -> None:
        if self._file is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            mode = "w" if self.pretty else "a"
            self._file = await aiofiles.open(self.path, mode, encoding="utf-8")
            if self.pretty:
                await self._file.write("[\n")

    async def _write_batch(self, records: list[dict]) -> None:
        await self._open()
        if self.pretty:
            parts = []
            for rec in records:
                body = json.dumps(rec, ensure_ascii=False, indent=2, default=_json_default)
                body = "  " + body.replace("\n", "\n  ")
                parts.append(("" if not self._written_any else ",\n") + body)
                self._written_any = True
            chunk = "".join(parts)
        else:
            chunk = "".join(
                json.dumps(rec, ensure_ascii=False, default=_json_default) + "\n"
                for rec in records
            )
        await self._file.write(chunk)
        await self._file.flush()

    async def _close(self) -> None:
        if self.pretty and self._file is None:
            await self._open()               # пустой массив тоже валидный JSON
        if self._file is not None:
            if self.pretty:
                await self._file.write("\n]\n")
            await self._file.close()
            self._file = None

    async def read_all(self) -> list[dict]:
        """Читает файл обратно — в любом из двух режимов."""
        async with aiofiles.open(self.path, "r", encoding="utf-8") as f:
            content = await f.read()
        if content.lstrip().startswith("["):
            return json.loads(content)
        return [json.loads(line) for line in content.splitlines() if line.strip()]


# ---------- CSV ----------

class CSVStorage(DataStorage):
    """
    CSV — таблица для Excel и pandas.

    Заголовки берутся из первой записи. Списки и словари (links, metadata)
    сохраняются в ячейке как JSON-строка.

    Запятые, кавычки и переносы строк внутри текста экранирует модуль csv.
    Через ",".join первая же запятая в заголовке страницы сломала бы таблицу.

    encoding — "utf-8" по умолчанию; "utf-8-sig" добавляет BOM, чтобы Excel
    правильно показал кириллицу.
    """

    def __init__(self, path: str | Path, encoding: str = "utf-8",
                 delimiter: str = ",", **kwargs) -> None:
        _require(aiofiles, "aiofiles")
        super().__init__(**kwargs)
        self.path = Path(path)
        self.encoding = encoding
        self.delimiter = delimiter
        self.fieldnames: list[str] | None = None
        self._file = None

    @staticmethod
    def _flatten(rec: dict) -> dict:
        out = {}
        for key, value in rec.items():
            if isinstance(value, (list, dict)):
                out[key] = json.dumps(value, ensure_ascii=False)
            elif isinstance(value, datetime):
                out[key] = value.isoformat()
            elif value is None:
                out[key] = ""
            else:
                out[key] = value
        return out

    async def _write_batch(self, records: list[dict]) -> None:
        rows = [self._flatten(r) for r in records]
        buf = io.StringIO()
        header_needed = self.fieldnames is None
        if header_needed:
            self.fieldnames = list(rows[0].keys())    # автоопределение заголовков
        writer = csv.DictWriter(buf, fieldnames=self.fieldnames, delimiter=self.delimiter,
                                extrasaction="ignore", lineterminator="\n")
        if header_needed:
            writer.writeheader()
        writer.writerows(rows)

        if self._file is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            # newline="" — чтобы переносы строк внутри ячеек не превращались в \r\n
            self._file = await aiofiles.open(self.path, "w", encoding=self.encoding, newline="")
        await self._file.write(buf.getvalue())
        await self._file.flush()

    async def _close(self) -> None:
        if self._file is not None:
            await self._file.close()
            self._file = None

    async def read_all(self) -> list[dict]:
        """Читает таблицу обратно и разворачивает JSON-ячейки и числа."""
        async with aiofiles.open(self.path, "r", encoding=self.encoding, newline="") as f:
            content = await f.read()
        rows = list(csv.DictReader(io.StringIO(content), delimiter=self.delimiter))
        for row in rows:
            for key in ("links", "metadata"):
                if row.get(key):
                    row[key] = json.loads(row[key])
            if row.get("status_code"):
                row["status_code"] = int(row["status_code"])
        return rows


# ---------- SQLite ----------

class SQLiteStorage(DataStorage):
    """
    SQLite — база в одном файле, без отдельного сервера.

    Таблица pages, одна строка на страницу. url уникален: повторно
    скачанная страница обновляет строку, а не дублирует её. links и metadata
    хранятся как JSON-текст. Индексы по url, crawled_at и status_code.

    Пачка вставляется одной транзакцией (executemany + commit) — это и есть
    batch-вставка из пункта 9.
    """

    SCHEMA = """
        CREATE TABLE IF NOT EXISTS pages (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            url           TEXT    NOT NULL UNIQUE,
            title         TEXT,
            text          TEXT,
            links         TEXT,          -- JSON-список
            metadata      TEXT,          -- JSON-объект
            crawled_at    TEXT,          -- дата в формате ISO
            status_code   INTEGER,
            content_type  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pages_crawled_at ON pages(crawled_at);
        CREATE INDEX IF NOT EXISTS idx_pages_status     ON pages(status_code);
    """

    UPSERT = """
        INSERT INTO pages (url, title, text, links, metadata, crawled_at, status_code, content_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(url) DO UPDATE SET
            title = excluded.title,         text = excluded.text,
            links = excluded.links,         metadata = excluded.metadata,
            crawled_at = excluded.crawled_at, status_code = excluded.status_code,
            content_type = excluded.content_type
    """

    def __init__(self, path: str | Path, **kwargs) -> None:
        _require(aiosqlite, "aiosqlite")
        super().__init__(**kwargs)
        self.path = Path(path)
        self._db: aiosqlite.Connection | None = None

    async def init_db(self) -> None:
        """Создаёт файл базы, таблицу и индексы. Повторный вызов безопасен."""
        if self._db is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.path)
        await self._db.executescript(self.SCHEMA)
        await self._db.commit()

    async def _write_batch(self, records: list[dict]) -> None:
        await self.init_db()
        assert self._db is not None
        rows = [
            (
                r["url"], r["title"], r["text"],
                json.dumps(r["links"], ensure_ascii=False),
                json.dumps(r["metadata"], ensure_ascii=False),
                r["crawled_at"].isoformat() if isinstance(r["crawled_at"], datetime)
                else r["crawled_at"],
                r["status_code"], r["content_type"],
            )
            for r in records
        ]
        try:
            await self._db.executemany(self.UPSERT, rows)
            await self._db.commit()
        except Exception:
            await self._db.rollback()      # пачка не должна записаться наполовину
            raise

    async def _close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def query(self, sql: str, params: tuple = ()) -> list[dict]:
        """Произвольный SELECT, результат — список словарей."""
        await self.init_db()
        assert self._db is not None
        async with self._db.execute(sql, params) as cursor:
            names = [d[0] for d in cursor.description]
            return [dict(zip(names, row)) for row in await cursor.fetchall()]

    async def read_all(self) -> list[dict]:
        rows = await self.query(
            "SELECT url, title, text, links, metadata, crawled_at, status_code, content_type "
            "FROM pages ORDER BY id"
        )
        for row in rows:
            row["links"] = json.loads(row["links"])
            row["metadata"] = json.loads(row["metadata"])
        return rows


# ---------- Несколько хранилищ сразу ----------

class MultiStorage(DataStorage):
    """
    Пишет одну запись сразу в несколько хранилищ — для демо (пункт 10):
    обойти сайт один раз и сохранить в три формата.

        storage = MultiStorage([JSONStorage("a.jsonl"), CSVStorage("a.csv")])

    Буферы и повторы у каждого вложенного хранилища свои.
    """

    def __init__(self, storages: list[DataStorage]) -> None:
        super().__init__(batch_size=1)
        self.storages = storages

    async def save(self, data: dict) -> None:
        record = to_record(data)
        await asyncio.gather(*(s.save(record) for s in self.storages))

    async def flush(self) -> None:
        await asyncio.gather(*(s.flush() for s in self.storages))

    async def _write_batch(self, records: list[dict]) -> None:   # не используется
        pass

    async def close(self) -> None:
        await asyncio.gather(*(s.close() for s in self.storages))

    def get_stats(self) -> dict:
        return {"storage": "MultiStorage", "parts": [s.get_stats() for s in self.storages]} 