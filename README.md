# project-Concurrency-and-asynchrony

Асинхронный веб-краулер на Python — учебный проект за семь дней.

Краулер обходит сайт по ссылкам или по sitemap.xml, качает десятки страниц
одновременно, соблюдает robots.txt и лимиты скорости, повторяет запросы при
временных ошибках, сохраняет результат в JSON, CSV или SQLite и строит
HTML-отчёт со статистикой. Настраивается файлом `config.yaml` или из
командной строки.

```
python -m crawler --urls https://books.toscrape.com/ --max-pages 50 \
    --output output/books.csv --report output/report.html
```

## Содержание

- [Стек](#стек)
- [Установка](#установка)
- [Быстрый старт](#быстрый-старт)
- [Командная строка](#командная-строка)
- [Руководство по конфигурации](#руководство-по-конфигурации)
- [Как это устроено](#как-это-устроено)
- [API](#api)
- [Проверки и производительность](#проверки-и-производительность)
- [Структура проекта](#структура-проекта)
- [Этапы](#этапы)

## Стек

| Библиотека | Зачем |
|---|---|
| `asyncio` | event loop, корутины, очереди, семафоры |
| `aiohttp` | асинхронные HTTP-запросы, пул соединений |
| `aiofiles` | асинхронная запись файлов |
| `beautifulsoup4` + `lxml` | разбор HTML |
| `aiosqlite` | асинхронная работа с SQLite |
| `pyyaml` | конфигурация в YAML |

## Установка

```bash
pip install -r requirements.txt
```

В GitHub Codespaces этого достаточно. Локально лучше в виртуальном окружении:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Быстрый старт

Все команды — из корня репозитория.

**1. Из командной строки**

```bash
python -m crawler --urls https://books.toscrape.com/ --max-pages 30 --output output/books.jsonl
```

Во время обхода — прогресс-бар, в конце — краткая сводка JSON.

**2. Через файл конфигурации**

```bash
python -m crawler --config config.yaml
python -m crawler --config config.yaml --max-pages 10     # с поправкой на лету
```

**3. Из Python**

```python
import asyncio
from crawler import AdvancedCrawler

async def main():
    crawler = AdvancedCrawler.from_config("config.yaml")
    await crawler.crawl()

    stats = crawler.get_stats()
    print(f"Обработано: {stats['total_pages']} страниц")
    print(f"Успешно: {stats['successful']}")
    print(f"Ошибок: {stats['failed']}")

    crawler.export_to_html_report("report.html")
    await crawler.close()

asyncio.run(main())
```

**4. Отдельные компоненты** — каждый работает и сам по себе:

```python
from crawler import AsyncCrawler, SitemapParser, JSONStorage, RetryStrategy

# внутри async def
crawler = AsyncCrawler(max_concurrent=5, requests_per_second=2, respect_robots=True,
                       retry_strategy=RetryStrategy(max_retries=3),
                       storage=JSONStorage("output/pages.jsonl"))
try:
    urls = await SitemapParser(fetcher=crawler.fetch_bytes).fetch_sitemap(
        "https://example.com/sitemap.xml")
    pages = await crawler.crawl(urls, max_pages=100)
finally:
    await crawler.close()
```

**Почему `python -m crawler`, а не `python crawler.py`.** `crawler` — папка-пакет.
Флаг `-m` запускает пакет как программу: Python выполняет `crawler/__main__.py`.
Файл `crawler.py` рядом с папкой конфликтовал бы с ней по имени. Так же — через
`-m` — запускаются демо: `python -m demos.demo_day7`. Без `-m` Python не нашёл бы
пакет `crawler`, потому что искал бы его в папке `demos/`.

## Командная строка

```bash
python -m crawler --help
```

| Параметр | Что делает |
|---|---|
| `--urls URL [URL ...]` | стартовые адреса |
| `--config ФАЙЛ` | конфигурация `.yaml`/`.yml`/`.json` |
| `--max-pages N` | сколько страниц обработать максимум |
| `--max-depth N` | глубина обхода от стартовых адресов |
| `--output ФАЙЛ` | куда сохранять страницы; формат по расширению: `.json`, `.jsonl`, `.csv`, `.db`/`.sqlite` |
| `--respect-robots` / `--no-respect-robots` | соблюдать robots.txt (или выключить, если включено в конфиге) |
| `--rate-limit RPS` | не больше стольких запросов в секунду |
| `--concurrency N` | одновременных запросов |
| `--sitemap URL [URL ...]` | адреса sitemap.xml |
| `--use-sitemap` | найти sitemap через robots.txt стартовых сайтов |
| `--same-domain` / `--no-same-domain` | не уходить на другие домены |
| `--report ФАЙЛ.html` | HTML-отчёт |
| `--stats-json ФАЙЛ.json` | статистика в JSON |
| `--log-file ФАЙЛ`, `--log-level LEVEL` | лог в файл с ротацией и его уровень |
| `--user-agent СТРОКА` | как представляться сайтам |
| `--user-agents СТРОКА [СТРОКА ...]` | несколько User-Agent для ротации |
| `--no-progress` | без прогресс-бара |

Приоритет настроек: **по умолчанию < файл `--config` < параметры командной строки**.

Код выхода: `0` — обход завершён, `2` — неверные параметры, `130` — остановлен Ctrl+C.

## Руководство по конфигурации

Формат — YAML или JSON, по расширению файла. Любую настройку можно не писать:
возьмётся значение по умолчанию. Опечатка в имени — ошибка с подсказкой
(`неизвестная настройка «crawler.max_concurent». Может быть, имелось в виду
«max_concurrent»?`), неверный тип или значение — тоже ошибка до старта обхода.
Полный пример с комментариями — [`config.yaml`](config.yaml).

**Верхний уровень**

| Настройка | По умолчанию | Описание |
|---|---|---|
| `start_urls` | `[]` | стартовые адреса |
| `sitemaps` | `[]` | адреса sitemap.xml; найденные страницы добавляются к стартовым |
| `use_sitemap` | `false` | искать sitemap самому: `Sitemap:` в robots.txt, иначе `/sitemap.xml` |

**`limits`** — объём обхода

| Настройка | По умолчанию | Описание |
|---|---|---|
| `max_pages` | `100` | сколько страниц обработать максимум |
| `max_depth` | `2` | 0 — только стартовые, 1 — плюс ссылки с них, … |

**`filters`** — какие ссылки брать

| Настройка | По умолчанию | Описание |
|---|---|---|
| `same_domain_only` | `true` | только домены стартовых адресов |
| `include_patterns` | `[]` | регулярные выражения: если заданы, адрес должен совпасть хоть с одним |
| `exclude_patterns` | `[]` | регулярные выражения: совпавшие адреса пропускаются |

**`crawler`** — сеть и вежливость

| Настройка | По умолчанию | Описание |
|---|---|---|
| `max_concurrent` | `10` | одновременных запросов всего |
| `max_per_domain` | `null` | одновременных запросов к одному домену; `null` — без отдельного лимита |
| `rate_limit` | `null` | запросов в секунду; `null` — без ограничения |
| `rate_per_domain` | `true` | у каждого домена свой лимит скорости |
| `min_delay` | `0` | минимальная пауза между запросами, c |
| `jitter` | `0` | случайная добавка к паузе, 0…jitter c |
| `error_backoff` | `0` | замедление после 429/5xx/таймаутов, растёт вдвое с каждой ошибкой подряд |
| `respect_robots` | `false` | соблюдать robots.txt, включая Crawl-delay |
| `user_agent` | `null` | заголовок User-Agent; `null` — стандартный aiohttp |
| `user_agents` | `[]` | ротация: каждый запрос берёт следующую строку по кругу; robots.txt проверяется для `user_agent`, а без него — для первой в списке |
| `connect_timeout`, `read_timeout`, `total_timeout` | `10`, `15`, `30` | таймауты, c |

**`retry`** — повторы при временных ошибках (408, 429, 5xx, таймауты, обрывы сети)

| Настройка | По умолчанию | Описание |
|---|---|---|
| `enabled` | `false` | включить повторы |
| `max_retries` | `3` | повторов максимум |
| `backoff_factor` | `2.0` | во сколько раз растёт пауза |
| `base_delay`, `max_delay` | `0.5`, `30` | первая и предельная пауза, c |

**`circuit_breaker`** — временная блокировка домена, который подряд отвечает ошибками

| Настройка | По умолчанию | Описание |
|---|---|---|
| `enabled` | `false` | включить |
| `failure_threshold` | `5` | ошибок подряд до блокировки |
| `recovery_timeout` | `30` | через сколько секунд пробовать снова |

**`storage`** — сохранение страниц

| Настройка | По умолчанию | Описание |
|---|---|---|
| `type` | `none` | `none`, `json` (массив), `jsonl` (строка на запись), `csv`, `sqlite` |
| `path` | — | путь к файлу; обязателен, если `type` не `none` |
| `batch_size` | `50` | сколько записей копить перед записью на диск |
| `overwrite` | `true` | начинать файл заново при каждом запуске |

**`logging`**

| Настройка | По умолчанию | Описание |
|---|---|---|
| `level` | `INFO` | уровень для файла: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `console_level` | `WARNING` | уровень для консоли |
| `file` | `null` | файл лога; `null` — только консоль |
| `max_bytes`, `backup_count` | `1000000`, `3` | ротация: размер файла и сколько старых хранить |
| `format` | `text` | `text` или `json` (запись на строку) |

**`output`** — отчёты сразу после обхода: `html_report`, `stats_json` (пути или `null`).

**`progress`** — `enabled` (`true`), `interval` (`0.5` c).

## Как это устроено

```
config.yaml / командная строка
        │
        ▼
AdvancedCrawler ──── логирование: файл + консоль, ротация
   │          ├──── SitemapParser: стартовые адреса из sitemap.xml
   │          ├──── CrawlerStats → JSON / HTML-отчёт
   │          └──── ProgressMonitor: прогресс-бар
   ▼
AsyncCrawler.crawl
   очередь URL (CrawlerQueue, приоритет по глубине, без дублей)
        │
        ├─→ воркер 1 ┐
        ├─→ воркер 2 ├─→ robots.txt → circuit breaker → семафоры
        └─→ воркер N ┘    → лимит скорости → запрос → повторы при ошибке
                                → разбор HTML → новые ссылки в очередь
                                → сохранение (JSON / CSV / SQLite)
```

Всё работает в одном потоке на одном event loop. Пока один воркер ждёт ответ
сервера, остальные отправляют свои запросы — отсюда выигрыш в скорости.
Ограничители (семафоры, лимит скорости) не дают этому параллелизму превратиться
в нагрузку на чужой сайт.

## API

Всё импортируется из пакета: `from crawler import ...`.

### AdvancedCrawler — день 7

| Метод | Что делает |
|---|---|
| `AdvancedCrawler.from_config(path_or_dict_or_config)` | создать из `config.yaml`, `.json`, словаря или `CrawlerConfig` |
| `await crawl(start_urls=None) -> dict` | обход; адреса из аргумента или конфигурации плюс sitemap. Возвращает `{url: данные}` успешных страниц. Каждый вызов — новый обход со своей статистикой; хранилище общее, второй обход дописывает в него |
| `get_stats() -> dict` | `total_pages`, `successful`, `failed`, `blocked_by_robots`, `pages_per_second`, `duration_seconds`, `status_codes`, `top_domains`, `errors_by_type`, `depth_distribution`, `timeline`, `failed_urls_detail`, `components` (очередь, лимиты, повторы, хранилище, sitemap) |
| `export_to_json(filename)` | статистика в JSON |
| `export_to_html_report(filename)` | HTML-отчёт: графики, таблицы, светлая и тёмная тема, без интернета |
| `await close()` | дописать хранилище, закрыть соединения. Есть `async with` |

### AsyncCrawler — дни 1–6

```python
AsyncCrawler(max_concurrent=10, max_depth=3, max_per_domain=None,
             connect_timeout=10, read_timeout=15, total_timeout=30,
             requests_per_second=None, rate_per_domain=True, min_delay=0, jitter=0,
             error_backoff=0, respect_robots=False, user_agent=None, user_agents=None,
             retry_strategy=None, circuit_breaker=None, storage=None, on_page_done=None)
```

| Метод | Что делает |
|---|---|
| `await fetch_url(url) -> str \| None` | одна страница; ошибок не бросает, причина — в `errors` |
| `await fetch_urls(urls) -> dict` | список параллельно, `{url: html}` удачных |
| `await fetch_once(url) -> str` | одна попытка, бросает `CrawlerError` — для своих стратегий повторов |
| `await fetch_bytes(url) -> (код, байты)` | сырые байты, например sitemap.xml.gz |
| `await fetch_and_parse(url) -> dict` | загрузить и разобрать: `title`, `text`, `links`, `metadata`, `images`, `headings`, `tables`, `lists`, `status_code`, `error` |
| `await crawl(start_urls, max_pages=100, same_domain_only=False, exclude_patterns=None, include_patterns=None)` | обход по ссылкам через очередь и воркеры |
| `snapshot() -> dict` | состояние идущего обхода: готово, в очереди, в работе, ошибок |
| `get_error_stats()`, `get_rate_stats()` | статистика ошибок и вежливости |
| `await close()` | закрыть сессию и хранилище |

### Остальные компоненты

| Класс | День | Главное |
|---|---|---|
| `HTMLParser(parser="lxml")` | 2 | `await parse_html(html, url)`, `extract_links`, `extract_text(selector)`, `extract_metadata` |
| `CrawlerQueue` | 3 | очередь с приоритетами и без дублей: `add_url`, `get_next`, `mark_processed/failed/skipped`, `get_stats` |
| `SemaphoreManager(max_concurrent, max_per_domain)` | 3 | `async with acquire(url)` — общий лимит и лимит на домен |
| `RateLimiter(requests_per_second, per_domain, min_delay, jitter, error_backoff)` | 4 | `await acquire(domain)`, `get_stats` |
| `RobotsParser` | 4 | `await fetch_robots(url)`, `can_fetch(url, ua)`, `get_crawl_delay(ua)` |
| `RetryStrategy(max_retries, backoff_factor, ...)` | 5 | `await execute_with_retry(coro_func, *args)`, `get_stats` |
| `CircuitBreaker(failure_threshold, recovery_timeout)` | 5 | `check`, `record_success/failure`, `get_stats` |
| `TransientError`, `RateLimitedError`, `PermanentError`, `NetworkError`, `ParseError`, `CircuitOpenError` | 5 | классы ошибок, общий предок `CrawlerError` |
| `JSONStorage`, `CSVStorage`, `SQLiteStorage`, `MultiStorage` | 6 | `await save(page)`, `flush`, `close`, `read_all`; у SQLite ещё `query(sql)` |
| `SitemapParser(fetcher, max_depth=3, max_urls=None)` | 7 | `await fetch_sitemap(url) -> list[str]`, `await discover(site_url)`; индексы, gzip, защита от циклов |
| `CrawlerStats` | 7 | `record_page(data)`, `get_stats`, `export_to_json`, `export_to_html_report` |
| `CrawlerConfig` | 7 | `from_file(path)`, `from_dict(d)`, `validate()`; ошибка — `ConfigError` |
| `setup_logging(level, console_level, log_file, max_bytes, backup_count, fmt)` | 7 | файл + консоль, ротация, text или json |
| `ProgressMonitor(source, max_pages)` | 7 | прогресс-бар: процент, скорость, оставшееся время, задачи в работе |

## Проверки и производительность

```bash
python -m tests.check_all       # 69 проверок всех семи дней, ~30 c, интернет не нужен
python -m demos.perf_day7       # синхронно против асинхронно, 100/500/1000 страниц, память
```

`check_all` поднимает локальные HTTP-серверы и проверяет требования каждого
дня со стороны сервера: сколько запросов пришло одновременно, сколько раз
запрошена каждая страница, с какими паузами. Запускать после любого изменения
в `crawler/`: новая логика не должна сломать старую.

`perf_day7` отвечает на пункт 10 задания. Сервер отвечает с задержкой 50 мс,
как настоящий сайт. Порядок цифр:

| Что | Результат |
|---|---|
| 100 страниц синхронно, по одной | ~5 c (≈ 100 × 50 мс ожидания) |
| 100 страниц асинхронно, 20 разом | ~0.5 c, в 10+ раз быстрее |
| 100 / 500 / 1000 страниц | время растёт линейно, скорость держится |
| Память | растёт линейно: результаты копятся в `processed_urls` |
| Узкое место | извлечение текста при разборе HTML: был второй полный разбор страницы, стал один проход по готовому дереву — в десятки раз быстрее, результат тот же |

## Структура проекта

```
.
├── crawler/                    # пакет краулера
│   ├── __init__.py             # публичный API
│   ├── __main__.py             # python -m crawler
│   ├── cli.py                  # командная строка (argparse)
│   ├── config.py               # конфигурация YAML/JSON
│   ├── advanced_crawler.py     # AdvancedCrawler — всё вместе
│   ├── async_crawler.py        # загрузка и обход
│   ├── html_parser.py          # разбор HTML
│   ├── crawler_queue.py        # очередь URL с приоритетами
│   ├── semaphore_manager.py    # лимиты одновременных запросов
│   ├── rate_limiter.py         # частота запросов, паузы, backoff
│   ├── robots_parser.py        # robots.txt
│   ├── errors.py               # типы ошибок
│   ├── retry_strategy.py       # повторы
│   ├── circuit_breaker.py      # блокировка «лежащего» домена
│   ├── storage.py              # JSON, CSV, SQLite
│   ├── sitemap_parser.py       # sitemap.xml
│   ├── stats.py                # статистика
│   ├── report.py               # HTML-отчёт
│   ├── logging_setup.py        # логирование в файл и консоль
│   └── progress.py             # прогресс-бар
├── demos/                      # демонстрации по дням
│   ├── demo_day1.py … demo_day7.py
│   └── perf_day7.py            # тесты производительности
├── tests/
│   └── check_all.py            # сквозная проверка всех дней
├── docs/
├── config.yaml                 # пример конфигурации
├── requirements.txt
└── .gitignore
```

Результаты демо и обходов попадают в `output/` — эта папка в `.gitignore`.

## Этапы

| День | Тема | Тег |
|---|---|---|
| 1 | Базовый асинхронный HTTP-клиент | `day1` |
| 2 | Парсинг HTML и извлечение данных | `day2` |
| 3 | Очереди и управление конкурентностью | `day3` |
| 4 | Rate limiting и правила вежливости | `day4` |
| 5 | Обработка ошибок и автоматические повторы | `day5` |
| 6 | Сохранение данных и работа с файлами | `day6` |
| 7 | Продвинутые возможности и финальная интеграция | `day7` |

Каждый этап — тег git. Посмотреть состояние любого дня и сравнить два дня:

```bash
git checkout day3          # посмотреть
git checkout main          # вернуться
git diff day6 day7         # что изменилось
```

Не сделано (пункт 12 задания, необязательный): прокси, cookies, рендеринг
JavaScript, распределённый краулинг на нескольких машинах.