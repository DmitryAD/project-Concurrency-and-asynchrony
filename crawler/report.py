"""
День 7, пункт 3 — HTML-отчёт по статистике обхода.

Одна самодостаточная страница: стили, графики (SVG) и скрипт
подсказок — внутри файла. Интернет для просмотра не нужен, файл
можно переслать или открыть через год.

Что на странице:
  - главная цифра и плитки: успешно, ошибок, скорость, время;
  - столбики «сколько страниц готово в каждую секунду»;
  - полосы: коды ответов, топ доменов, ошибки по типам, глубина;
  - у каждого графика под ним таблица с теми же числами.

Все строки, пришедшие из сети (адреса, домены, тексты ошибок),
экранируются через html.escape: страница с адресом вида
<script>... не должна превратиться в работающий скрипт в отчёте.
"""

from __future__ import annotations

import html
import math

# Цвета — по ролям, светлая и тёмная тема. Данные — один синий ряд.
CSS = """
:root {
  color-scheme: light;
  --page: #f9f9f7; --surface: #fcfcfb; --ink: #0b0b0b; --ink-2: #52514e;
  --muted: #898781; --grid: #e1e0d9; --axis: #c3c2b7; --ring: rgba(11,11,11,0.10);
  --series: #2a78d6; --series-hover: #256abf; --good: #006300; --critical: #d03b3b;
}
@media (prefers-color-scheme: dark) {
  :root:not([data-theme="light"]) {
    color-scheme: dark;
    --page: #0d0d0d; --surface: #1a1a19; --ink: #ffffff; --ink-2: #c3c2b7;
    --muted: #898781; --grid: #2c2c2a; --axis: #383835; --ring: rgba(255,255,255,0.10);
    --series: #3987e5; --series-hover: #5598e7; --good: #0ca30c; --critical: #e66767;
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --page: #0d0d0d; --surface: #1a1a19; --ink: #ffffff; --ink-2: #c3c2b7;
  --muted: #898781; --grid: #2c2c2a; --axis: #383835; --ring: rgba(255,255,255,0.10);
  --series: #3987e5; --series-hover: #5598e7; --good: #0ca30c; --critical: #e66767;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--page); color: var(--ink);
  font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
main { max-width: 1040px; margin: 0 auto; padding: 24px 16px 48px; }
h1 { font-size: 22px; margin: 0 0 4px; font-weight: 600; }
h2 { font-size: 15px; margin: 0 0 2px; font-weight: 600; }
.sub { color: var(--ink-2); margin: 0 0 20px; }
.note { color: var(--ink-2); font-size: 13px; margin: 0 0 12px; }
.card { background: var(--surface); border: 1px solid var(--ring); border-radius: 12px;
  padding: 16px 18px; margin-bottom: 16px; min-width: 0; }
.hero { display: flex; flex-wrap: wrap; gap: 16px 32px; align-items: flex-end; }
.hero .big { font-size: 52px; font-weight: 600; line-height: 1; }
.hero .label { color: var(--ink-2); }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px;
  margin-bottom: 16px; }
.tile { background: var(--surface); border: 1px solid var(--ring); border-radius: 12px; padding: 12px 14px; }
.tile .label { color: var(--ink-2); font-size: 13px; }
.tile .value { font-size: 24px; font-weight: 600; margin-top: 2px; }
.tile .hint { color: var(--muted); font-size: 12px; }
.ok::before { content: "✓ "; color: var(--good); }
.bad::before { content: "✕ "; color: var(--critical); }
.grid2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }
.grid2 .card { margin-bottom: 0; }
.bars { display: grid; grid-template-columns: minmax(80px, max-content) 1fr auto; gap: 6px 10px;
  align-items: center; margin-top: 12px; }
.bars .name { color: var(--ink-2); font-size: 13px; overflow-wrap: anywhere; }
.bars .track { height: 18px; position: relative; }
.bars .fill { height: 100%; background: var(--series); border-radius: 0 4px 4px 0; min-width: 2px; }
.bars .hit { position: absolute; inset: -4px 0; cursor: default; }
.bars .hit:hover + .fill, .bars .hit:focus + .fill { background: var(--series-hover); }
.bars .val { font-variant-numeric: tabular-nums; font-size: 13px; text-align: right; }
svg { display: block; width: 100%; height: auto; overflow: visible; }
svg .gridline { stroke: var(--grid); stroke-width: 1; }
svg .baseline { stroke: var(--axis); stroke-width: 1; }
svg .tick { fill: var(--muted); font-size: 11px; font-variant-numeric: tabular-nums; }
svg .col { fill: var(--series); pointer-events: none; }
svg .hitcol { fill: transparent; }
svg .hitcol:hover + .col, svg .hitcol:focus + .col { fill: var(--series-hover); }
details { margin-top: 12px; }
summary { cursor: pointer; color: var(--ink-2); font-size: 13px; }
.table-wrap { overflow-x: auto; }
table { border-collapse: collapse; width: 100%; margin-top: 8px; font-size: 13px; }
th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid var(--grid); vertical-align: top; }
td.num, th.num { text-align: right; font-variant-numeric: tabular-nums; }
td.url { overflow-wrap: anywhere; }
.empty { color: var(--muted); margin: 12px 0 0; }
#tip { position: fixed; pointer-events: none; z-index: 10; display: none;
  background: var(--surface); color: var(--ink); border: 1px solid var(--ring);
  border-radius: 8px; padding: 6px 10px; font-size: 13px; box-shadow: 0 4px 16px rgba(0,0,0,.12); }
#tip b { display: block; font-size: 15px; }
#tip span { color: var(--ink-2); }
"""

# Подсказка при наведении. Текст кладётся через textContent — даже
# если в данных окажется HTML, он не выполнится.
JS = """
(function () {
  var tip = document.getElementById('tip');
  function show(el, x, y) {
    tip.replaceChildren();
    var b = document.createElement('b'); b.textContent = el.getAttribute('data-value');
    var s = document.createElement('span'); s.textContent = el.getAttribute('data-label');
    tip.append(b, s); tip.style.display = 'block';
    var r = tip.getBoundingClientRect();
    tip.style.left = Math.min(x + 14, window.innerWidth - r.width - 8) + 'px';
    tip.style.top = Math.max(y - r.height - 10, 8) + 'px';
  }
  document.querySelectorAll('[data-value]').forEach(function (el) {
    el.addEventListener('pointermove', function (e) { show(el, e.clientX, e.clientY); });
    el.addEventListener('pointerleave', function () { tip.style.display = 'none'; });
    el.addEventListener('focus', function () {
      var r = el.getBoundingClientRect(); show(el, r.left + r.width / 2, r.top);
    });
    el.addEventListener('blur', function () { tip.style.display = 'none'; });
  });
})();
"""


def esc(value) -> str:
    return html.escape(str(value), quote=True)


def fmt_int(n) -> str:
    return f"{n:,}".replace(",", " ") if isinstance(n, int) else esc(n)


def fmt_seconds(s: float) -> str:
    s = float(s or 0)
    if s < 60:
        return f"{s:.1f} с"
    m, sec = divmod(int(round(s)), 60)
    h, m = divmod(m, 60)
    return f"{h} ч {m:02d} мин" if h else f"{m} мин {sec:02d} с"


def nice_step(max_value: float, ticks: int = 4) -> float:
    """Шаг сетки из ряда 1, 2, 5 × 10^k, чтобы подписи были круглыми."""
    if max_value <= 0:
        return 1
    raw = max_value / ticks
    power = 10 ** math.floor(math.log10(raw))
    for m in (1, 2, 5, 10):
        if raw <= m * power:
            return max(1, m * power)
    return 10 * power


# ---------- куски страницы ----------

def tile(label: str, value: str, hint: str = "", cls: str = "") -> str:
    hint_html = f'<div class="hint">{esc(hint)}</div>' if hint else ""
    return (f'<div class="tile"><div class="label {cls}">{esc(label)}</div>'
            f'<div class="value">{value}</div>{hint_html}</div>')


def table(headers: list[str], rows: list[list], numeric: set[int] = frozenset()) -> str:
    head = "".join(f'<th class="{"num" if i in numeric else ""}">{esc(h)}</th>'
                   for i, h in enumerate(headers))
    body = "".join(
        "<tr>" + "".join(
            f'<td class="{"num" if i in numeric else "url"}">{fmt_int(c) if i in numeric else esc(c)}</td>'
            for i, c in enumerate(row)) + "</tr>"
        for row in rows
    )
    return (f'<details><summary>Таблица</summary><div class="table-wrap"><table>'
            f'<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div></details>')


def hbars(title: str, note: str, items: list[tuple[str, int]], unit: str,
          empty: str, col_name: str) -> str:
    """Горизонтальные полосы: подпись слева, полоса, число справа."""
    if not items:
        return (f'<section class="card"><h2>{esc(title)}</h2>'
                f'<p class="empty">{esc(empty)}</p></section>')
    top = max(v for _, v in items) or 1
    rows = []
    for name, value in items:
        width = 100 * value / top
        rows.append(
            f'<div class="name">{esc(name)}</div>'
            f'<div class="track"><div class="hit" tabindex="0" data-value="{fmt_int(value)} {esc(unit)}" '
            f'data-label="{esc(name)}"></div><div class="fill" style="width:{width:.2f}%"></div></div>'
            f'<div class="val">{fmt_int(value)}</div>'
        )
    return (f'<section class="card"><h2>{esc(title)}</h2><p class="note">{esc(note)}</p>'
            f'<div class="bars">{"".join(rows)}</div>'
            f'{table([col_name, unit], [[n, v] for n, v in items], numeric={1})}</section>')


def timeline_chart(timeline: dict) -> str:
    """Столбики: сколько страниц завершилось в каждом отрезке времени."""
    pages = timeline.get("pages", [])
    failed = timeline.get("failed", [0] * len(pages))
    bin_s = timeline.get("bin_seconds", 1)
    title = "Скорость по ходу обхода"
    note = (f"Сколько страниц завершилось за каждые {bin_s} с. "
            "Провалы — паузы вежливости, медленные ответы или повторы.")
    if not pages:
        return (f'<section class="card"><h2>{title}</h2>'
                f'<p class="empty">Страниц не было — показывать нечего.</p></section>')

    W, H = 720, 220
    left, right, top, bottom = 36, 8, 10, 26
    pw, ph = W - left - right, H - top - bottom
    step = nice_step(max(pages))
    ymax = step * math.ceil(max(pages) / step) or 1
    band = pw / len(pages)
    bar_w = max(1.0, min(24.0, band - 2))       # 2px просвета между соседями
    radius = min(4.0, bar_w / 2)

    parts = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{esc(title)}">']
    tick = 0
    while tick <= ymax + 1e-9:
        y = top + ph - ph * tick / ymax
        cls = "baseline" if tick == 0 else "gridline"
        parts.append(f'<line class="{cls}" x1="{left}" x2="{W - right}" y1="{y:.1f}" y2="{y:.1f}"/>')
        parts.append(f'<text class="tick" x="{left - 6}" y="{y + 4:.1f}" text-anchor="end">{int(tick)}</text>')
        tick += step

    label_every = max(1, math.ceil(len(pages) / 8))
    for i, value in enumerate(pages):
        x = left + band * i + (band - bar_w) / 2
        h = ph * value / ymax
        y0 = top + ph
        t0, t1 = i * bin_s, (i + 1) * bin_s
        fail_note = f", из них с ошибкой {failed[i]}" if failed[i] else ""
        parts.append(
            f'<rect class="hitcol" tabindex="0" x="{left + band * i:.1f}" y="{top}" '
            f'width="{band:.1f}" height="{ph}" data-value="{value} стр." '
            f'data-label="{t0}–{t1} с{fail_note}"/>'
        )
        if value > 0:
            r = min(radius, h)
            # Прямоугольник со скруглённым верхом и прямым низом
            parts.append(
                f'<path class="col" d="M{x:.1f},{y0:.1f} V{y0 - h + r:.1f} '
                f'Q{x:.1f},{y0 - h:.1f} {x + r:.1f},{y0 - h:.1f} H{x + bar_w - r:.1f} '
                f'Q{x + bar_w:.1f},{y0 - h:.1f} {x + bar_w:.1f},{y0 - h + r:.1f} V{y0:.1f} Z"/>'
            )
        else:
            parts.append('<path class="col" d=""/>')
        if i % label_every == 0:
            parts.append(f'<text class="tick" x="{left + band * i + band / 2:.1f}" y="{H - 8}" '
                         f'text-anchor="middle">{t0} с</text>')
    parts.append("</svg>")

    rows = [[f"{i * bin_s}–{(i + 1) * bin_s} с", p, f] for i, (p, f) in enumerate(zip(pages, failed))]
    return (f'<section class="card"><h2>{title}</h2><p class="note">{esc(note)}</p>{"".join(parts)}'
            f'{table(["Отрезок", "Страниц", "С ошибкой"], rows, numeric={1, 2})}</section>')


def failures_table(failed: dict) -> str:
    if not failed:
        return ""
    rows = [[url, info.get("type", ""), info.get("status") or "—", info.get("reason", "")]
            for url, info in list(failed.items())[:200]]
    head = "".join(f"<th>{h}</th>" for h in ("Адрес", "Тип", "Код", "Причина"))
    body = "".join("<tr>" + "".join(f'<td class="url">{esc(c)}</td>' for c in r) + "</tr>"
                   for r in rows)
    more = (f'<p class="note">Показаны первые 200 из {len(failed)}.</p>'
            if len(failed) > 200 else "")
    return (f'<section class="card"><h2>Неудачные адреса</h2>{more}<div class="table-wrap">'
            f'<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div></section>')


# ---------- страница целиком ----------

def render_html_report(stats: dict, title: str = "Отчёт краулера") -> str:
    """stats — словарь из CrawlerStats.get_stats() (плюс необязательные поля)."""
    total = stats.get("total_pages", 0)
    ok, bad = stats.get("successful", 0), stats.get("failed", 0)
    rate = stats.get("success_rate", 0.0)

    started = (stats.get("started_at") or "—").replace("T", " ")
    finished = (stats.get("finished_at") or "—").replace("T", " ")
    start_urls = stats.get("start_urls") or []
    source = ", ".join(start_urls[:3]) + (f" и ещё {len(start_urls) - 3}" if len(start_urls) > 3 else "")

    hero = (f'<section class="card hero"><div><div class="big">{fmt_int(total)}</div>'
            f'<div class="label">страниц обработано</div></div>'
            f'<div class="label">успешно {rate:.1f}% · {fmt_seconds(stats.get("duration_seconds", 0))} · '
            f'{stats.get("pages_per_second", 0):.2f} стр/с в среднем</div></section>')

    tiles = "".join([
        tile("Успешно", fmt_int(ok), f"{rate:.1f}% от обработанных", "ok"),
        tile("Ошибок", fmt_int(bad), "после всех повторов", "bad" if bad else ""),
        tile("Запрещено robots.txt", fmt_int(stats.get("blocked_by_robots", 0)), "не запрашивались"),
        tile("Средняя скорость", f'{stats.get("pages_per_second", 0):.2f}', "страниц в секунду"),
        tile("Время работы", esc(fmt_seconds(stats.get("duration_seconds", 0))), f"начало в {started.split(' ')[-1]}"),
        tile("Доменов", fmt_int(stats.get("domains_total", 0)), "с которых были страницы"),
    ])

    status = list(stats.get("status_codes", {}).items())
    domains = [(d["domain"], d["pages"]) for d in stats.get("top_domains", [])]
    errors = list(stats.get("errors_by_type", {}).items())
    depths = [(f"глубина {d}", n) for d, n in stats.get("depth_distribution", {}).items()]

    body = [
        f"<h1>{esc(title)}</h1>",
        f'<p class="sub">{esc(started)} — {esc(finished)}'
        + (f" · старт: {esc(source)}" if source else "") + "</p>",
        hero,
        f'<div class="tiles">{tiles}</div>',
        timeline_chart(stats.get("timeline", {})),
        '<div class="grid2">',
        hbars("Коды ответов", "Чем ответил сервер. «нет ответа» — запрос не дошёл: таймаут, сеть.",
              status, "страниц", "Ответов не было.", "Код"),
        hbars("Топ доменов", "Больше всего страниц с этих доменов.",
              domains, "страниц", "Доменов нет.", "Домен"),
        hbars("Ошибки по типам", "Итоговые ошибки — уже после повторов.",
              errors, "страниц", "Ошибок не было.", "Тип"),
        hbars("Глубина", "Сколько страниц на каждом шаге от стартовых.",
              depths, "страниц", "Нет данных о глубине.", "Глубина"),
        "</div><div style='height:16px'></div>",
        failures_table(stats.get("failed_urls_detail") or {}),
    ]
    return (
        "<!doctype html>\n<html lang=\"ru\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
        f"<title>{esc(title)}</title><style>{CSS}</style></head>"
        f"<body><main>{''.join(body)}</main><div id=\"tip\" role=\"tooltip\"></div>"
        f"<script>{JS}</script></body></html>\n"
    )