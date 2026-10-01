"""
День 7, пункт 4 — конфигурационный файл.

Все настройки обхода — в одном файле YAML (или JSON), а не в коде.
Поменять сайт, лимиты или формат сохранения можно без правки Python.

Файл делится на разделы. Любой раздел и любую настройку можно
не писать — тогда берётся значение по умолчанию из классов ниже.
Опечатка в имени настройки — ошибка с подсказкой, а не тихое
игнорирование: «max_concurent» иначе просто не сработал бы, и
краулер молча работал бы с 10 потоками вместо задуманных 3.

Полный пример с комментариями — config.yaml в корне проекта.
"""

from __future__ import annotations

import difflib
import json
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path


class ConfigError(ValueError):
    """Ошибка в конфигурации: неизвестная настройка, неверное значение."""


# ---------- разделы ----------

@dataclass
class LimitsConfig:
    max_pages: int = 100            # сколько страниц обработать максимум
    max_depth: int = 2              # насколько далеко уходить от стартовых


@dataclass
class FiltersConfig:
    same_domain_only: bool = True   # не уходить на чужие домены
    include_patterns: list[str] = field(default_factory=list)   # регулярки «только такие»
    exclude_patterns: list[str] = field(default_factory=list)   # регулярки «кроме таких»


@dataclass
class CrawlerSection:
    max_concurrent: int = 10
    max_per_domain: int | None = None
    rate_limit: float | None = None         # запросов в секунду, None — без лимита
    rate_per_domain: bool = True
    min_delay: float = 0.0
    jitter: float = 0.0
    error_backoff: float = 0.0
    respect_robots: bool = False
    user_agent: str | None = None
    connect_timeout: float = 10.0
    read_timeout: float = 15.0
    total_timeout: float = 30.0


@dataclass
class RetryConfig:
    enabled: bool = False
    max_retries: int = 3
    backoff_factor: float = 2.0
    base_delay: float = 0.5
    max_delay: float = 30.0


@dataclass
class CircuitBreakerConfig:
    enabled: bool = False
    failure_threshold: int = 5
    recovery_timeout: float = 30.0


STORAGE_TYPES = ("none", "json", "jsonl", "csv", "sqlite")


@dataclass
class StorageConfig:
    type: str = "none"              # none | json | jsonl | csv | sqlite
    path: str | None = None
    batch_size: int = 50
    overwrite: bool = True          # начать файл заново при каждом запуске


LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


@dataclass
class LoggingConfig:
    level: str = "INFO"             # что пишется в файл
    console_level: str = "WARNING"  # что видно в консоли
    file: str | None = None         # None — без файла
    max_bytes: int = 1_000_000      # размер файла до ротации
    backup_count: int = 3           # сколько старых файлов хранить
    format: str = "text"            # text | json


@dataclass
class OutputConfig:
    html_report: str | None = None  # куда положить HTML-отчёт
    stats_json: str | None = None   # куда положить статистику в JSON


@dataclass
class ProgressConfig:
    enabled: bool = True
    interval: float = 0.5           # как часто обновлять, секунды


@dataclass
class CrawlerConfig:
    start_urls: list[str] = field(default_factory=list)
    sitemaps: list[str] = field(default_factory=list)   # адреса sitemap.xml
    use_sitemap: bool = False       # искать sitemap у стартовых сайтов сам
    limits: LimitsConfig = field(default_factory=LimitsConfig)
    filters: FiltersConfig = field(default_factory=FiltersConfig)
    crawler: CrawlerSection = field(default_factory=CrawlerSection)
    retry: RetryConfig = field(default_factory=RetryConfig)
    circuit_breaker: CircuitBreakerConfig = field(default_factory=CircuitBreakerConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    progress: ProgressConfig = field(default_factory=ProgressConfig)

    # ---------- загрузка ----------

    @classmethod
    def from_file(cls, path: str | Path) -> "CrawlerConfig":
        """Читает .yaml/.yml или .json — по расширению файла."""
        path = Path(path)
        if not path.exists():
            raise ConfigError(f"файл конфигурации не найден: {path}")
        text = path.read_text(encoding="utf-8")

        if path.suffix.lower() in (".yaml", ".yml"):
            try:
                import yaml
            except ImportError as e:
                raise ConfigError("для YAML нужен пакет pyyaml: pip install pyyaml") from e
            try:
                data = yaml.safe_load(text) or {}
            except yaml.YAMLError as e:
                raise ConfigError(f"{path}: не разбирается как YAML: {e}") from e
        elif path.suffix.lower() == ".json":
            try:
                data = json.loads(text) if text.strip() else {}
            except json.JSONDecodeError as e:
                raise ConfigError(f"{path}: не разбирается как JSON: {e}") from e
        else:
            raise ConfigError(f"{path}: поддерживаются .yaml, .yml и .json")

        if not isinstance(data, dict):
            raise ConfigError(f"{path}: на верхнем уровне ожидался словарь настроек")
        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: dict) -> "CrawlerConfig":
        config = _build(cls, data, prefix="")
        config.validate()
        return config

    def to_dict(self) -> dict:
        return asdict(self)

    # ---------- проверка значений ----------

    def validate(self) -> None:
        problems = []
        if self.limits.max_pages < 1:
            problems.append("limits.max_pages должен быть не меньше 1")
        if self.limits.max_depth < 0:
            problems.append("limits.max_depth не может быть отрицательным")
        if self.crawler.max_concurrent < 1:
            problems.append("crawler.max_concurrent должен быть не меньше 1")
        if self.crawler.rate_limit is not None and self.crawler.rate_limit <= 0:
            problems.append("crawler.rate_limit должен быть больше 0 (или null — без лимита)")
        if self.storage.type not in STORAGE_TYPES:
            problems.append(f"storage.type: '{self.storage.type}', а можно {', '.join(STORAGE_TYPES)}")
        if self.storage.type != "none" and not self.storage.path:
            problems.append(f"storage.path обязателен для storage.type = {self.storage.type}")
        for name in ("level", "console_level"):
            value = getattr(self.logging, name)
            if str(value).upper() not in LOG_LEVELS:
                problems.append(f"logging.{name}: '{value}', а можно {', '.join(LOG_LEVELS)}")
        if self.logging.format not in ("text", "json"):
            problems.append("logging.format: можно text или json")
        for url in self.start_urls + self.sitemaps:
            if not str(url).startswith(("http://", "https://")):
                problems.append(f"адрес должен начинаться с http:// или https://: {url}")
        if problems:
            raise ConfigError("ошибки в конфигурации:\n  - " + "\n  - ".join(problems))


# ---------- сборка dataclass из словаря ----------

_NUMBER_TYPES = {"int": (int,), "float": (int, float)}


def _build(cls, data: dict, prefix: str):
    """
    Создаёт экземпляр cls из словаря, проверяя каждое имя и тип.
    Вложенные разделы собираются рекурсивно.
    """
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise ConfigError(f"{prefix.rstrip('.') or 'конфигурация'}: ожидался раздел (словарь)")

    known = {f.name: f for f in fields(cls)}
    kwargs = {}
    for key, value in data.items():
        if key not in known:
            hint = difflib.get_close_matches(key, known, n=1)
            tip = f" Может быть, имелось в виду «{hint[0]}»?" if hint else ""
            raise ConfigError(f"неизвестная настройка «{prefix}{key}».{tip}")

        f = known[key]
        default = f.default_factory() if callable(f.default_factory) else f.default
        if hasattr(default, "__dataclass_fields__"):      # вложенный раздел
            kwargs[key] = _build(type(default), value, prefix=f"{prefix}{key}.")
        else:
            kwargs[key] = _check_type(f"{prefix}{key}", f.type, value)
    return cls(**kwargs)


def _check_type(name: str, annotation: str, value):
    """Мягкая проверка типа по аннотации. YAML уже даёт числа и списки."""
    ann = str(annotation)
    if value is None:
        if "None" in ann:
            return None
        raise ConfigError(f"{name}: значение не может быть пустым")

    base = ann.split("|")[0].strip()
    if base == "bool":
        if not isinstance(value, bool):
            raise ConfigError(f"{name}: ожидалось true или false, получено {value!r}")
    elif base in _NUMBER_TYPES:
        if isinstance(value, bool) or not isinstance(value, _NUMBER_TYPES[base]):
            raise ConfigError(f"{name}: ожидалось число, получено {value!r}")
    elif base == "str":
        if not isinstance(value, str):
            raise ConfigError(f"{name}: ожидалась строка, получено {value!r}")
    elif base.startswith("list"):
        if isinstance(value, str):
            value = [value]                 # одна строка вместо списка — простим
        if not isinstance(value, list):
            raise ConfigError(f"{name}: ожидался список, получено {value!r}")
    return value


def load_config(path: str | Path) -> CrawlerConfig:
    """Короткий вариант CrawlerConfig.from_file."""
    return CrawlerConfig.from_file(path)