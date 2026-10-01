"""
День 2. Парсинг HTML и извлечение структурированных данных.

HTMLParser в сеть не ходит: получает готовый HTML и возвращает словарь
с данными. Загрузка — задача AsyncCrawler, разбор — этого класса.
"""

from __future__ import annotations

import logging
from urllib.parse import urljoin, urldefrag, urlparse

from bs4 import BeautifulSoup
from bs4.element import CData, NavigableString, Tag

try:                                    # есть в beautifulsoup4 >= 4.10
    from bs4.element import TemplateString
except ImportError:  # pragma: no cover
    TemplateString = NavigableString

logger = logging.getLogger(__name__)

# Схемы, которые не являются страницами: почта, телефон, скрипты
SKIP_SCHEMES = {"mailto", "tel", "javascript", "data", "about"}

# Теги, чей текст не является содержимым страницы
NON_CONTENT_TAGS = ("script", "style", "noscript", "template")
_SKIP_TAGS = frozenset(NON_CONTENT_TAGS)

# комментарии, DOCTYPE и script/style у BeautifulSoup других типов и в текст не попадут
_TEXT_TYPES = (NavigableString, CData, TemplateString)


class HTMLParser:
    """
    Разбирает HTML в структурированные данные.

        parser = HTMLParser()
        data = await parser.parse_html(html, "https://example.com")
    """

    def __init__(
        self,
        parser: str = "lxml",
        same_domain_only: bool = False,
    ) -> None:
        """
        parser — движок разбора: "lxml" быстрее, "html.parser" есть всегда.
            Если lxml не установлен, переключаюсь на встроенный.
        same_domain_only — пункт 4, необязательная фильтрация внешних ссылок.
            По умолчанию выключена.
        """
        self.parser = self._resolve_parser(parser)
        self.same_domain_only = same_domain_only

    @staticmethod
    def _resolve_parser(preferred: str) -> str:
        if preferred == "lxml":
            try:
                import lxml  # noqa: F401
            except ImportError:
                logger.warning("lxml не установлен, использую html.parser")
                return "html.parser"
        return preferred

    # ---------- главный метод ----------

    async def parse_html(self, html: str, url: str) -> dict:
        """
        Разбирает HTML и возвращает словарь со всеми извлечёнными данными.

        async — по заданию и для удобства вызова из асинхронного кода, хотя
        внутри только вычисления, без ожидания ввода-вывода.

        Пункт 6: каждое извлечение в своём try, ошибки копятся в parse_errors,
        и при сбое одного блока остальные всё равно возвращаются.
        """
        result: dict = {
            "url": url,
            "title": "",
            "text": "",
            "links": [],
            "metadata": {},
            "images": [],
            "headings": {},
            "tables": [],
            "lists": [],
            "parse_errors": [],
        }

        # BeautifulSoup строит дерево почти из любого мусора; если всё же упал — дальше не с чем
        try:
            soup = BeautifulSoup(html, self.parser)
        except Exception as e:  # noqa: BLE001
            logger.error("Не удалось разобрать HTML для %s: %s", url, e)
            result["parse_errors"].append(f"BeautifulSoup: {type(e).__name__}: {e}")
            return result

        # каждое извлечение отдельно: падение одного не мешает остальным
        extractors = (
            ("metadata", lambda: self.extract_metadata(soup)),
            ("text", lambda: self.extract_text(soup)),
            ("links", lambda: self.extract_links(soup, url)),
            ("images", lambda: self.extract_images(soup, url)),
            ("headings", lambda: self.extract_headings(soup)),
            ("tables", lambda: self.extract_tables(soup)),
            ("lists", lambda: self.extract_lists(soup)),
        )

        for field, extract in extractors:
            try:
                result[field] = extract()
            except Exception as e:  # noqa: BLE001
                logger.warning("Ошибка извлечения '%s' на %s: %s", field, url, e)
                result["parse_errors"].append(f"{field}: {type(e).__name__}: {e}")

        # title дублируем на верхний уровень — он нужен почти всегда
        result["title"] = result["metadata"].get("title", "")

        logger.info(
            "Разобрано %s — ссылок: %d, текста: %d символов",
            url, len(result["links"]), len(result["text"]),
        )
        return result

    # ---------- ссылки ----------

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """
        Все ссылки страницы в абсолютном виде.

        Пункт 4: конвертация относительных ссылок, валидация и необязательная
        фильтрация внешних.
        """
        base_domain = urlparse(base_url).netloc
        links: list[str] = []
        seen: set[str] = set()

        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if not href:
                continue

            # не страницы: mailto:, tel:, javascript: и якоря "#section"
            scheme = href.split(":", 1)[0].lower() if ":" in href else ""
            if scheme in SKIP_SCHEMES or href.startswith("#"):
                continue

            # "/about" — от корня, "page2" — от текущей папки, "../up" — уровнем выше
            absolute = urljoin(base_url, href)

            # page#intro и page#outro — одна страница
            absolute, _ = urldefrag(absolute)

            if not self._is_valid_url(absolute):
                continue

            if self.same_domain_only and urlparse(absolute).netloc != base_domain:
                continue

            # без дублей, с сохранением порядка
            if absolute not in seen:
                seen.add(absolute)
                links.append(absolute)

        return links

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        """Валидация из пункта 4: только http(s) и только с доменом."""
        try:
            parts = urlparse(url)
        except ValueError:
            return False
        return parts.scheme in ("http", "https") and bool(parts.netloc)

    # ---------- текст ----------

    def extract_text(self, soup: BeautifulSoup, selector: str | None = None) -> str:
        """
        Достаёт видимый текст страницы.

        selector — CSS-селектор ("article", "div.content", "#main").
        Если не задан, берётся текст всей страницы.
        """
        if selector:
            blocks = soup.select(selector)
            if not blocks:
                logger.warning("Селектор '%s' ничего не нашёл", selector)
                return ""
            return "\n".join(self._clean_text(b) for b in blocks).strip()

        return self._clean_text(soup)

    @staticmethod
    def _clean_text(node) -> str:
        """
        Текст узла без служебных тегов (<script>, <style> и т. п.) — иначе
        в текст попадёт JS и CSS, а это бывает половина всех символов.

        Оптимизация дня 7: раньше дерево превращалось обратно в строку и
        разбиралось второй раз, чтобы удалить теги на копии, — 58% времени
        всего разбора. Теперь один проход по готовому дереву без захода
        в служебные теги. Результат тот же, на странице 70 КБ: 79 мс → 1.2 мс.
        """
        if isinstance(node, Tag) and node.name in _SKIP_TAGS:
            return ""

        parts: list[str] = []
        stack = [node]
        while stack:                         # обход в глубину без рекурсии
            current = stack.pop()
            if isinstance(current, Tag):
                if current is not node and current.name in _SKIP_TAGS:
                    continue                 # внутрь служебного тега не идём
                stack.extend(reversed(current.contents))
            elif type(current) in _TEXT_TYPES:
                text = current.strip()
                if text:
                    parts.append(text)

        # пробел между кусками, иначе слова слипаются на границе тегов
        return " ".join(parts)

    # ---------- метаданные ----------

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        """Пункт 2 задания: title, description, keywords."""
        metadata: dict = {"title": "", "description": "", "keywords": ""}

        if soup.title and soup.title.string:
            # "Travel |\n    Books" -> "Travel | Books"
            metadata["title"] = " ".join(soup.title.string.split())

        for name in ("description", "keywords"):
            # attrs={"name": ...} — ищем <meta name="description" content="...">
            tag = soup.find("meta", attrs={"name": name})
            if tag and tag.get("content"):
                metadata[name] = tag["content"].strip()

        return metadata

    # ---------- пункт 5: специфичные данные ----------

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        """Все картинки с src (абсолютным) и alt."""
        images: list[dict] = []
        for tag in soup.find_all("img"):
            src = (tag.get("src") or "").strip()
            if not src:
                continue
            images.append({
                "src": urljoin(base_url, src),
                "alt": (tag.get("alt") or "").strip(),
            })
        return images

    def extract_headings(self, soup: BeautifulSoup) -> dict:
        """Заголовки h1, h2, h3 — по ним видно структуру страницы."""
        return {
            level: [h.get_text(strip=True) for h in soup.find_all(level)]
            for level in ("h1", "h2", "h3")
        }

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        """
        Таблицы страницы: список строк, строка — список ячеек.
        Берутся и th, и td, поэтому первая строка обычно шапка.
        """
        tables: list[list[list[str]]] = []
        for table in soup.find_all("table"):
            rows: list[list[str]] = []
            for tr in table.find_all("tr"):
                cells = [
                    cell.get_text(strip=True)
                    for cell in tr.find_all(["th", "td"])
                ]
                if cells:
                    rows.append(cells)
            if rows:
                tables.append(rows)
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[list[str]]:
        """Маркированные и нумерованные списки."""
        lists: list[list[str]] = []
        for tag in soup.find_all(["ul", "ol"]):
            # только прямые потомки, иначе вложенный список попадёт и во внешний
            items = [li.get_text(strip=True) for li in tag.find_all("li", recursive=False)]
            items = [i for i in items if i]
            if items:
                lists.append(items)
        return lists