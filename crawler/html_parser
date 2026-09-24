"""
День 2. Парсинг HTML и извлечение структурированных данных.

Класс HTMLParser не ходит в сеть. Он получает готовую строку с HTML
и превращает её в словарь с данными. Разделение намеренное: загрузка —
дело AsyncCrawler, разбор — дело этого класса.
"""

from __future__ import annotations

import logging
from urllib.parse import urljoin, urldefrag, urlparse

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Схемы, которые не являются страницами: почта, телефон, скрипты
SKIP_SCHEMES = {"mailto", "tel", "javascript", "data", "about"}

# Теги, чей текст не является содержимым страницы
NON_CONTENT_TAGS = ("script", "style", "noscript", "template")


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
        parser — движок разбора: "lxml" быстрее, "html.parser" всегда
        доступен без внешних зависимостей. Если lxml не установлен,
        молча переключаемся на встроенный.

        same_domain_only — пункт 4 задания, необязательная фильтрация
        внешних ссылок. По умолчанию выключена.
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

        Метод объявлен async, потому что так требует задание и потому что
        так его удобно вызывать из асинхронного кода. Но внутри ничего
        асинхронного нет: разбор HTML — вычислительная работа, а не
        ожидание ввода-вывода.

        Пункт 6 задания: сбой одного извлечения не отменяет остальные.
        Каждый блок обёрнут отдельно, ошибки копятся в parse_errors,
        а на выходе получается частичный, но валидный результат.
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

        # BeautifulSoup крайне снисходителен к битому HTML: незакрытые
        # теги, перепутанная вложенность, мусор вместо разметки — он всё
        # равно построит дерево. Поэтому исключение здесь маловероятно,
        # но если оно случится, дальше идти уже не с чем.
        try:
            soup = BeautifulSoup(html, self.parser)
        except Exception as e:  # noqa: BLE001
            logger.error("Не удалось разобрать HTML для %s: %s", url, e)
            result["parse_errors"].append(f"BeautifulSoup: {type(e).__name__}: {e}")
            return result

        # Каждое извлечение — отдельная попытка. Упало одно,
        # остальные всё равно отработают.
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
        Собирает все ссылки со страницы и приводит их к абсолютному виду.

        Пункт 4 задания целиком: конвертация относительных ссылок,
        валидация и необязательная фильтрация внешних.
        """
        base_domain = urlparse(base_url).netloc
        links: list[str] = []
        seen: set[str] = set()

        for tag in soup.find_all("a", href=True):
            href = tag["href"].strip()
            if not href:
                continue

            # Отсекаем то, что не является страницей:
            # mailto:, tel:, javascript: и якоря вида "#section"
            scheme = href.split(":", 1)[0].lower() if ":" in href else ""
            if scheme in SKIP_SCHEMES or href.startswith("#"):
                continue

            # urljoin — вся магия превращения относительной ссылки
            # в абсолютную. Он знает правила: "/about" считается от корня
            # сайта, "page2" — от текущей папки, "../up" — на уровень выше.
            absolute = urljoin(base_url, href)

            # Отрезаем якорь: page#intro и page#outro — одна и та же
            # страница, качать её дважды незачем
            absolute, _ = urldefrag(absolute)

            if not self._is_valid_url(absolute):
                continue

            if self.same_domain_only and urlparse(absolute).netloc != base_domain:
                continue

            # Дедупликация с сохранением порядка: set быстро проверяет
            # повтор, list хранит очерёдность
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
        Выковыривает текст, предварительно выбросив служебные теги.

        Без этого в «текст страницы» попадёт содержимое <script> —
        то есть JavaScript-код, — и <style> с правилами CSS. На реальных
        сайтах это легко половина всех символов.
        """
        # Работаем на копии, чтобы не портить дерево для других методов
        copy = BeautifulSoup(str(node), "html.parser")
        for tag in copy(NON_CONTENT_TAGS):
            tag.decompose()

        # separator=" " не даёт словам слипнуться на границе тегов:
        # <b>при</b><i>вет</i> без него станет "привет", а не "при вет"
        return copy.get_text(separator=" ", strip=True)

    # ---------- метаданные ----------

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        """Пункт 2 задания: title, description, keywords."""
        metadata: dict = {"title": "", "description": "", "keywords": ""}

        if soup.title and soup.title.string:
            metadata["title"] = soup.title.string.strip()

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
        Таблицы страницы.

        Каждая таблица — список строк, каждая строка — список ячеек.
        Берём и th (заголовки), и td (данные): первая строка обычно
        оказывается шапкой.
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
            # recursive=False — берём только прямых потомков, иначе
            # пункты вложенного списка попадут и во внешний тоже
            items = [li.get_text(strip=True) for li in tag.find_all("li", recursive=False)]
            items = [i for i in items if i]
            if items:
                lists.append(items)
        return lists
