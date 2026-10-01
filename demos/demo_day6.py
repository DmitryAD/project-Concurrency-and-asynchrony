"""
День 6 — демонстрация (пункты 10 и 11 задания).

    pip install -r requirements.txt     # появился aiosqlite
    python -m demos.demo_day6

1. Обход books.toscrape.com с сохранением сразу в JSON, CSV и SQLite.
2. Статистика по сохранённому.
3. Чтение обратно из всех трёх форматов, запросы к базе.
4. Замер: пакетная запись против записи по одной.
5. Ошибка записи не останавливает обход (на локальном тестовом сервере).

Файлы появятся в output/day6/ — эта папка в .gitignore.
"""

import asyncio
import logging
import time
from pathlib import Path

from crawler import AsyncCrawler, CSVStorage, JSONStorage, MultiStorage, SQLiteStorage
from tests.check_all import Handler, start_server

OUT = Path("output") / "day6"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(name)-15s | %(message)s",
        datefmt="%H:%M:%S",
    )
    for name in ("crawler.async_crawler", "crawler.html_parser",
                 "crawler.robots_parser", "aiohttp"):
        logging.getLogger(name).setLevel(logging.WARNING)


def header(title: str) -> None:
    print(f"\n{'=' * 72}\n  {title}\n{'=' * 72}")


def size(path: Path) -> str:
    return f"{path.stat().st_size / 1024:.1f} КБ" if path.exists() else "нет файла"


async def demo_crawl_and_save() -> None:
    """1-3. Обход, сохранение в три формата, чтение обратно."""
    header("1. Обход books.toscrape.com с сохранением в JSON, CSV и SQLite")

    for f in ("pages.jsonl", "pages.csv", "pages.db"):
        (OUT / f).unlink(missing_ok=True)       # каждый запуск — с чистого листа

    json_st = JSONStorage(OUT / "pages.jsonl", batch_size=10)
    csv_st = CSVStorage(OUT / "pages.csv", encoding="utf-8-sig", batch_size=10)
    db_st = SQLiteStorage(OUT / "pages.db", batch_size=10)

    crawler = AsyncCrawler(
        max_concurrent=5,
        max_depth=2,
        requests_per_second=3,
        respect_robots=True,
        user_agent="MyBot/1.0 (educational project)",
        storage=MultiStorage([json_st, csv_st, db_st]),
    )
    started = time.perf_counter()
    try:
        results = await crawler.crawl(
            ["https://books.toscrape.com/"], max_pages=25, same_domain_only=True,
        )
    finally:
        await crawler.close()           # дописывает буферы и закрывает файлы
    print(f"\n  Обработано страниц: {len(results)} за {time.perf_counter() - started:.1f} c")

    header("2. Что сохранилось")
    print(f"\n  {'Хранилище':<14}{'записей':>8}{'пачек':>7}{'ошибок':>8}   файл")
    for st, path in ((json_st, OUT / "pages.jsonl"), (csv_st, OUT / "pages.csv"),
                     (db_st, OUT / "pages.db")):
        s = st.get_stats()
        print(f"  {s['storage']:<14}{s['saved']:>8}{s['batches']:>7}{s['failed']:>8}   "
              f"{path} ({size(path)})")

    header("3. Чтение обратно")

    from_json = await JSONStorage(OUT / "pages.jsonl").read_all()
    from_csv = await CSVStorage(OUT / "pages.csv", encoding="utf-8-sig").read_all()
    from_db = await SQLiteStorage(OUT / "pages.db").read_all()
    same = ({r["url"] for r in from_json} == {r["url"] for r in from_csv}
            == {r["url"] for r in from_db} == set(results))
    print(f"\n  JSON: {len(from_json)}, CSV: {len(from_csv)}, SQLite: {len(from_db)} записей")
    print(f"  Во всех трёх одни и те же адреса, и они совпадают с обходом: {'да' if same else 'НЕТ'}")

    first = from_json[0]
    print("\n  Первая запись из JSON:")
    for key in ("url", "title", "status_code", "content_type", "crawled_at"):
        print(f"    {key:<13}: {first[key]}")
    print(f"    {'links':<13}: {len(first['links'])} ссылок, первая {first['links'][:1]}")

    db = SQLiteStorage(OUT / "pages.db")
    try:
        print("\n  SQL: страницы с наибольшим числом ссылок")
        rows = await db.query(
            "SELECT title, json_array_length(links) AS n FROM pages ORDER BY n DESC LIMIT 3"
        )
        for r in rows:
            print(f"    {r['n']:>4}  {r['title'][:60]}")

        print("\n  SQL: поиск по заголовку (категории про путешествия и историю)")
        rows = await db.query(
            "SELECT title FROM pages WHERE title LIKE ? OR title LIKE ?", ("%Travel%", "%History%")
        )
        for r in rows:
            print(f"    {r['title'][:70]}")

        rows = await db.query("SELECT status_code, COUNT(*) AS n FROM pages GROUP BY status_code")
        print(f"\n  SQL: коды ответов: {rows}")
    finally:
        await db.close()

    print("\n  Таблицу можно открыть в pandas одной строкой:")
    print("    pd.read_csv('output/day6/pages.csv')")
    print("    pd.read_sql('SELECT * FROM pages', sqlite3.connect('output/day6/pages.db'))")


async def demo_batch_speed() -> None:
    """4. Скорость: пачками против по одной."""
    header("4. Скорость: 1000 записей в SQLite — по одной и пачками")
    records = [
        {"url": f"https://speed.test/{i}", "title": f"Страница {i}", "text": "x" * 500,
         "links": [f"https://speed.test/{i + 1}"], "metadata": {}, "status_code": 200}
        for i in range(1000)
    ]
    print()
    for batch_size in (1, 100):
        path = OUT / f"speed_{batch_size}.db"
        path.unlink(missing_ok=True)
        st = SQLiteStorage(path, batch_size=batch_size)
        t = time.perf_counter()
        for r in records:
            await st.save(r)
        await st.close()
        print(f"  batch_size={batch_size:<4}: {time.perf_counter() - t:6.2f} c, "
              f"пачек {st.batches}")
        path.unlink(missing_ok=True)
    print("\n    По одной — 1000 транзакций и 1000 записей на диск.")
    print("    Пачками по 100 — 10. Разница обычно в разы, иногда в десятки раз.")


async def demo_storage_error() -> None:
    """5. Ошибка записи."""
    header("5. Хранилище сломано, обход продолжается")
    broken = OUT / "broken_target"
    broken.mkdir(parents=True, exist_ok=True)     # на месте файла — папка
    print(f"\n  Пишем в {broken} — это папка, записать туда файл нельзя.\n")

    server1, B = start_server()
    server2, B2 = start_server()
    Handler.external = f"{B2}/site/x"
    st = JSONStorage(broken, batch_size=5, write_retries=1, retry_delay=0.1)
    crawler = AsyncCrawler(max_depth=5, storage=st)
    try:
        results = await crawler.crawl([f"{B}/site/"], same_domain_only=True)
    finally:
        await crawler.close()
        server1.shutdown()
        server2.shutdown()

    s = st.get_stats()
    print(f"\n  Обход: обработано {len(results)} страниц — до конца")
    print(f"  Хранилище: записано {s['saved']}, потеряно {s['failed']}, "
          f"попыток записи с ошибкой {s['write_errors']}")
    print("    Каждая пачка: попытка, повтор, запись в лог — и дальше.")


async def main() -> None:
    setup_logging()
    OUT.mkdir(parents=True, exist_ok=True)
    await demo_crawl_and_save()
    await demo_batch_speed()
    await demo_storage_error()
    print("\nГотово.\n")


if __name__ == "__main__":
    asyncio.run(main())