"""
День 2 — демонстрация (пункты 7 и 8 задания).

    pip install -r requirements.txt
    python -m demos.demo_day2

Пункт 7 — демонстрация:
    загружаем и парсим несколько реальных страниц,
    показываем структурированный итог и статистику.

Пункт 8 — тестирование:
    валидный HTML, битый HTML, извлечение ссылок,
    конвертация относительных URL.

Проверки из пункта 8 работают на локальных строках с HTML,
без обращения к сети: результат должен быть одинаковым при каждом
запуске, а реальные сайты меняются.
"""

import asyncio
import json
import logging

from crawler import AsyncCrawler, HTMLParser


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


def header(title: str) -> None:
    print(f"\n{'=' * 64}\n  {title}\n{'=' * 64}")


async def demo_fetch_and_parse() -> None:
    """Пункт 7. Загрузка и разбор реальных страниц."""
    header("Пункт 7. Загрузка и разбор реальных страниц")

    urls = [
        "https://example.com",
        "https://www.python.org",
        "https://docs.aiohttp.org/en/stable/",
    ]

    crawler = AsyncCrawler(max_concurrent=3)
    try:
        results = await asyncio.gather(
            *(crawler.fetch_and_parse(url) for url in urls)
        )
    finally:
        await crawler.close()

    for data in results:
        print(f"\n  {'─' * 60}")

        if data["error"]:
            print(f"  {data['url']}\n    НЕ ЗАГРУЗИЛОСЬ: {data['error']}")
            continue

        # Сводка в том виде, как показано в примере задания
        summary = {
            "url": data["url"],
            "title": data["title"],
            "text_length": len(data["text"]),
            "links_count": len(data["links"]),
            "images_count": len(data["images"]),
            "links": data["links"][:3],
        }
        print(json.dumps(summary, ensure_ascii=False, indent=4))

        # Заголовки — по ним видна структура страницы
        h1 = data["headings"].get("h1", [])
        h2 = data["headings"].get("h2", [])
        if h1:
            print(f"    h1: {h1[:3]}")
        if h2:
            print(f"    h2: {h2[:3]}")

        desc = data["metadata"].get("description", "")
        if desc:
            print(f"    description: {desc[:80]}...")

        if data["parse_errors"]:
            print(f"    предупреждения при разборе: {data['parse_errors']}")

    # Общая статистика
    ok = [d for d in results if not d["error"]]
    print(f"\n  {'─' * 60}")
    print(f"  Разобрано страниц : {len(ok)} из {len(urls)}")
    print(f"  Всего ссылок      : {sum(len(d['links']) for d in ok)}")
    print(f"  Всего картинок    : {sum(len(d['images']) for d in ok)}")
    print(f"  Всего текста      : {sum(len(d['text']) for d in ok)} символов")


# Пункт 8. Валидный HTML

VALID_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Тестовая страница</title>
    <meta name="description" content="Страница для проверки парсера">
    <meta name="keywords" content="python, asyncio, парсинг">
    <style>body { color: red; }</style>
    <script>var secret = "меня не должно быть в тексте";</script>
</head>
<body>
    <h1>Главный заголовок</h1>
    <p>Первый абзац текста.</p>
    <h2>Раздел один</h2>
    <p>Второй абзац.</p>
    <h3>Подраздел</h3>

    <a href="https://example.com/page1">Абсолютная ссылка</a>
    <a href="/about">Ссылка от корня</a>
    <a href="contacts.html">Относительная ссылка</a>

    <img src="/img/logo.png" alt="Логотип">
    <img src="https://cdn.example.com/photo.jpg" alt="Фото">
    <img src="" alt="Пустая, должна быть пропущена">

    <table>
        <tr><th>Товар</th><th>Цена</th></tr>
        <tr><td>Кофемашина</td><td>45000</td></tr>
        <tr><td>Кофемолка</td><td>12000</td></tr>
    </table>

    <ul>
        <li>Первый пункт</li>
        <li>Второй пункт</li>
    </ul>
    <ol>
        <li>Раз</li>
        <li>Два</li>
        <li>Три</li>
    </ol>
</body>
</html>
"""


async def test_valid_html() -> None:
    header("Пункт 8. Парсинг валидного HTML")

    parser = HTMLParser()
    data = await parser.parse_html(VALID_HTML, "https://test.local/docs/index.html")

    print(f"\n  title       : {data['title']}")
    print(f"  description : {data['metadata']['description']}")
    print(f"  keywords    : {data['metadata']['keywords']}")

    print(f"\n  Заголовки:")
    for level, items in data["headings"].items():
        if items:
            print(f"    {level}: {items}")

    print(f"\n  Ссылки ({len(data['links'])}):")
    for link in data["links"]:
        print(f"    {link}")

    print(f"\n  Картинки ({len(data['images'])}):")
    for img in data["images"]:
        print(f"    {img['src']}  (alt: {img['alt']})")

    print(f"\n  Таблицы ({len(data['tables'])}):")
    for row in data["tables"][0]:
        print(f"    {row}")

    print(f"\n  Списки ({len(data['lists'])}):")
    for lst in data["lists"]:
        print(f"    {lst}")

    print(f"\n  Текст ({len(data['text'])} символов):")
    print(f"    {data['text'][:100]}")

    # содержимое <script> и <style> не должно попасть в текст
    assert "secret" not in data["text"], "JavaScript просочился в текст!"
    assert "color: red" not in data["text"], "CSS просочился в текст!"
    print("\n    Содержимое <script> и <style> в текст не попало.")


# Пункт 8. Битый HTML

BROKEN_HTML = """
<html>
<head><title>Битая страница</title>
<body>
    <h1>Заголовок без закрывающего тега
    <p>Абзац <b>с незакрытым жирным
    <a href="/page">Ссылка без закрытия
    <a href=/no-quotes>Атрибут без кавычек</a>
    <table>
        <tr><td>Ячейка без закрытия
        <tr><td>Вторая строка
    </table>
    <ul><li>Пункт один<li>Пункт два</ul>
    <div><span>Перепутанная </div>вложенность</span>
    <p>Хвост страницы
"""

GARBAGE = "просто текст, вообще не похожий на HTML >>> <<< &&& ??? "


async def test_broken_html() -> None:
    header("Пункт 8. Битый HTML")

    print("\n  Случай 1: незакрытые теги, атрибут без кавычек,")
    print("  перепутанная вложенность — обычное дело на реальных сайтах.\n")

    parser = HTMLParser()
    data = await parser.parse_html(BROKEN_HTML, "https://broken.local/")

    print(f"    title   : {data['title']!r}")
    print(f"    h1      : {data['headings'].get('h1')}")
    print(f"    ссылки  : {data['links']}")
    print(f"    таблицы : {data['tables']}")
    print(f"    списки  : {data['lists']}")
    print(f"    ошибки  : {data['parse_errors']}")

    assert data["parse_errors"] == [], "Разбор битого HTML дал ошибки"
    assert data["title"] == "Битая страница"
    assert len(data["links"]) == 2, data["links"]
    assert data["tables"], "Таблица не извлеклась"
    assert data["lists"], "Список не извлёкся"
    print("\n    Данные извлеклись несмотря на поломанную разметку:")
    print("    BeautifulSoup достраивает недостающие закрывающие теги сам.")

    print("\n  Случай 2: на входе вообще не HTML.\n")

    data = await parser.parse_html(GARBAGE, "https://garbage.local/")
    print(f"    title   : {data['title']!r}")
    print(f"    ссылки  : {data['links']}")
    print(f"    текст   : {data['text'][:40]!r}")
    print(f"    ошибки  : {data['parse_errors']}")

    assert data["parse_errors"] == []
    assert data["links"] == []
    print("\n    Пустой результат вместо исключения — программа жива.")


# Пункт 8. Конвертация относительных ссылок

RELATIVE_HTML = """
<html><body>
    <a href="https://other.com/external">абсолютная чужая</a>
    <a href="/from-root">от корня сайта</a>
    <a href="sibling.html">соседний файл</a>
    <a href="../parent.html">на уровень выше</a>
    <a href="./same.html">явно текущая папка</a>
    <a href="//cdn.example.com/x">без схемы</a>
    <a href="#anchor">якорь</a>
    <a href="mailto:me@example.com">почта</a>
    <a href="tel:+79001234567">телефон</a>
    <a href="javascript:void(0)">скрипт</a>
    <a href="/from-root">дубликат</a>
    <a href="/page#section1">с якорем</a>
</body></html>
"""


async def test_relative_links() -> None:
    header("Пункт 8. Конвертация относительных ссылок в абсолютные")

    base = "https://shop.example.com/catalog/coffee/page.html"
    print(f"\n  Базовый URL: {base}\n")

    parser = HTMLParser()
    data = await parser.parse_html(RELATIVE_HTML, base)

    print("  Получилось:")
    for link in data["links"]:
        print(f"    {link}")

    expected = [
        "https://other.com/external",
        "https://shop.example.com/from-root",
        "https://shop.example.com/catalog/coffee/sibling.html",
        "https://shop.example.com/catalog/parent.html",
        "https://shop.example.com/catalog/coffee/same.html",
        "https://cdn.example.com/x",
        "https://shop.example.com/page",
    ]
    assert data["links"] == expected, f"\nОжидалось:\n{expected}\nПолучено:\n{data['links']}"

    print("\n  Что отброшено:")
    print("    #anchor          — якорь на этой же странице")
    print("    mailto:, tel:    — не веб-страницы")
    print("    javascript:      — не веб-страница")
    print("    дубликат /from-root — повтор")
    print("    /page#section1   — якорь отрезан, осталось /page")


async def test_same_domain_filter() -> None:
    """Пункт 8. Фильтр внешних ссылок (пункт 4, опционально)."""
    header("Пункт 8. Фильтр внешних ссылок")

    base = "https://shop.example.com/catalog/coffee/page.html"

    parser = HTMLParser(same_domain_only=True)
    data = await parser.parse_html(RELATIVE_HTML, base)

    print(f"\n  same_domain_only=True, домен: shop.example.com\n")
    for link in data["links"]:
        print(f"    {link}")

    assert all("shop.example.com" in link for link in data["links"])
    print("\n    other.com и cdn.example.com отфильтрованы.")


async def main() -> None:
    setup_logging()
    await test_valid_html()
    await test_broken_html()
    await test_relative_links()
    await test_same_domain_filter()
    await demo_fetch_and_parse()
    print("\nГотово.\n")


if __name__ == "__main__":
    asyncio.run(main())