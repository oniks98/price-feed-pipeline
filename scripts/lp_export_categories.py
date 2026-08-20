"""
lp_export_categories.py
════════════════════════════════════════════════════════════════════════
КРОК 1 пайплайну маппінгу LP → Prom.

Що робить:
  1. Завантажує повне дерево категорій LP
     (GET /external/catalog/category/list/tree) і будує індекс
     code → повний breadcrumb-шлях (root → ... → leaf, uk, fallback ru).
  2. Пагінує GET /external/catalog/product/list/all (batch 500),
     фільтрує товари виробників TARGET_MANUFACTURERS, збирає унікальні
     листові категорії та підставляє їм повний breadcrumb із дерева (1).
  3. Об'єднує результат із наявним data/lp/lp_category.csv (ідемпотентно):
       - файлу немає → записується з нуля (заголовок + нові рядки);
       - категорія вже є у файлі (за парою channel + category id) →
         рядок лишається незміненим, ручні правки збережені;
       - категорії немає у файлі → рядок дописується в кінець
         (sequential №, сортування нових рядків за breadcrumb);
       - категорії з EXCLUDED_CATEGORY_CODES (напр. Уцінка 12261, Рекламна продукція 12864):
           · ніколи не додаються з API;
           · якщо вже є у файлі — видаляються одноразово при першому запуску
             після включення до EXCLUDED_CATEGORY_CODES; подальші запуски
             файл не перезаписують.

Заповнює автоматично (тільки для нових рядків):
  col 16 (category id)              — code категорії LP
  col 17 (Назва у постачальника)    — повний breadcrumb, напр.
                                       "Системи безпеки > Відеоспостереження > Відеореєстратори"

Решта колонок — порожні, заповнює вручну або lp_map_categories.py.

Запуск (можна виконувати повторно — ідемпотентно):
  python scripts/lp_export_categories.py
"""

from __future__ import annotations

import csv
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

import requests
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
_ENV_FILE     = _PROJECT_ROOT / "suppliers" / ".env"
_OUTPUT_CSV   = _PROJECT_ROOT / "data" / "lp" / "lp_category.csv"

CSV_DELIMITER = ";"
CSV_ENCODING  = "utf-8-sig"

PAGE_SIZE     = 500
REQUEST_DELAY = 0.15   # seconds between successful pages
MAX_RETRIES   = 3
RETRY_BACKOFF = 2.0    # seconds (multiplied by attempt number)

# ─────────────────────────────────────────────────────────────────────
# DOMAIN CONSTANTS
# ─────────────────────────────────────────────────────────────────────
TARGET_MANUFACTURERS: frozenset[str] = frozenset({
    "logicpower",
    "greenvision",
})

# Category codes that must never appear in the export.
# - Existing rows with these codes are dropped on the next run.
# - New occurrences during collection are skipped entirely.
# "12261" = Уцінка (списання/розпродаж) — не товарна категорія.
EXCLUDED_CATEGORY_CODES: frozenset[str] = frozenset({
    "12261",  # Уцінка
    "12864",  # Рекламна продукція
    "12356",  # Системи безпеки > Витратні матеріали
    # Системи безпеки > СКУД
    "12358",  # > Ключі
    # Комп'ютерні комплектуючі та периферія
    "11265",  # > Клавіатури
    "12271",  # > Комп'ютерні корпуси
    # Електроніка та аксесуари
    "11887",  # > Кабелі та перехідники
    # Мережеве обладнання
    "12345",  # > Інструмент
    "12363",  # > Пасивне мережеве обладнання > Мережеві конектори, розетки, модулі
    "12362",  # > Пасивне мережеве обладнання > Патч-корди
    "12335",  # > Електроустаткування > Стабілізатори напруги
    "11259",  # Комплекти СЕС
})

# Статуси, які вважаються доступними для обробки категорій.
# Must stay in sync with suppliers/spiders/lp/api.py → ALLOWED_STATUSES.
ALLOWED_STATUSES: frozenset[str] = frozenset({
    "inStock",          # В наявності
    "quickProduction",  # Швидке виробництво
})

CHANNELS: tuple[str, ...] = ("site", "prom")

# Column names — single source of truth; matches lp_map_categories.py
CSV_HEADER: list[str] = [
    "№",                              # 0
    "Линк категории поставщика",      # 1
    "channel",                        # 2
    "prefix",                         # 3
    "coef",                           # 4
    "threshold",                      # 5
    "Номер_групи",                    # 6
    "Назва_групи",                    # 7
    "Ідентифікатор_підрозділу",       # 8  ← filled by lp_map_categories.py
    "Посилання_підрозділу",           # 9  ← filled by lp_map_categories.py
    "Особисті_нотатки",               # 10
    "Ярлик",                          # 11
    "Назва_Характеристики",           # 12
    "Одиниця_виміру_Характеристики",  # 13
    "Значення_Характеристики",        # 14
    "feed",                           # 15
    "category id",                    # 16 ← code категорії LP (e.g. "12324")
    "Назва у постачальника",          # 17 ← повний breadcrumb з дерева категорій LP
    "Назва_Характеристики",           # 18
    "Одиниця_виміру_Характеристики",  # 19
    "Значення_Характеристики",        # 20
]

_COL_IDX: dict[str, int] = {name: i for i, name in enumerate(CSV_HEADER)}


# ─────────────────────────────────────────────────────────────────────
# DATA MODEL
# ─────────────────────────────────────────────────────────────────────
@dataclass(frozen=True, slots=True)
class CategoryTreeInfo:
    """Flattened node of the LP category tree."""
    breadcrumb: str   # full path root → ... → this node, e.g.
                       # "Системи безпеки > Відеоспостереження > Відеореєстратори"
    slug_uk:    str   # this node's slug (uk, fallback ru)


@dataclass(frozen=True, slots=True)
class CategoryRecord:
    code:       str   # LP category code — used as key and written to col 16
    name_uk:    str   # leaf name in Ukrainian
    breadcrumb: str   # full path from the category tree (col 17)
    slug_uk:    str   # leaf slug for optional frontend link construction


@dataclass
class ExistingCsvData:
    """Parsed contents of a previously written lp_category.csv."""
    rows:        list[list[str]]              # data rows (excluded codes already dropped)
    index:       dict[tuple[str, str], int]   # (channel, category id) -> position in `rows`
    max_row_num: int                           # highest "№" seen (0 if file absent/empty)
    n_excluded:  int = 0                       # rows dropped because of EXCLUDED_CATEGORY_CODES


# ─────────────────────────────────────────────────────────────────────
# HTTP SESSION
# ─────────────────────────────────────────────────────────────────────
def build_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "X-Api-Key":       token,
        "Accept":          "application/json",
        "Accept-Encoding": "gzip, deflate",
    })
    return s


# ─────────────────────────────────────────────────────────────────────
# API PAGINATION  (streaming — no full dataset in memory)
# ─────────────────────────────────────────────────────────────────────
def _assert_ok(payload: dict[str, Any], context: str) -> None:
    if not payload.get("status"):
        sys.exit(f"❌ API status=false [{context}]: {payload}")


def _fetch_page(
    session: requests.Session,
    endpoint: str,
    page_num: int,
) -> dict[str, Any]:
    params = {"pageSize": PAGE_SIZE, "pageNum": page_num}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(endpoint, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()  # type: ignore[no-any-return]
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                sys.exit(
                    f"❌ Page {page_num} failed after {MAX_RETRIES} attempts: {exc}"
                )
            wait = RETRY_BACKOFF * attempt
            log.warning(
                "Retry %d/%d (page=%d): %s — waiting %.1fs",
                attempt, MAX_RETRIES, page_num, exc, wait,
            )
            time.sleep(wait)
    raise RuntimeError("unreachable")


def iter_products(
    session: requests.Session,
    base_url: str,
) -> Iterator[dict[str, Any]]:
    """Yield products one by one; never holds more than one page in memory."""
    endpoint   = f"{base_url.rstrip('/')}/external/catalog/product/list/all"
    page       = 1
    total_seen = 0
    total_items: int | None = None

    while True:
        payload = _fetch_page(session, endpoint, page)
        _assert_ok(payload, f"page={page}")

        data  = payload.get("data") or {}
        items: list[dict[str, Any]] = data.get("items") or []

        if total_items is None:
            total_items = int(data.get("totalItems") or 0)
            total_pages = (total_items + PAGE_SIZE - 1) // PAGE_SIZE or 1
            log.info("📦 Всього товарів: %d | сторінок: %d", total_items, total_pages)

        if not items:
            log.info("Порожня сторінка %d — зупиняємось.", page)
            break

        for item in items:
            yield item

        total_seen += len(items)
        log.info(
            "Сторінка %-3d → %d / %d",
            page, total_seen, total_items,
        )

        if total_items and total_seen >= total_items:
            break

        page += 1
        time.sleep(REQUEST_DELAY)


# ─────────────────────────────────────────────────────────────────────
# CATEGORY TREE  (code → full breadcrumb path)
# ─────────────────────────────────────────────────────────────────────
def fetch_category_tree(
    session: requests.Session,
    base_url: str,
) -> list[dict[str, Any]]:
    """
    GET /external/catalog/category/list/tree — повне дерево категорій LP.
    Без параметрів. Повертає список кореневих вузлів з children[].
    """
    endpoint = f"{base_url.rstrip('/')}/external/catalog/category/list/tree"
    log.info("🌳 Завантажуємо дерево категорій...")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(endpoint, timeout=30)
            resp.raise_for_status()
            payload: dict[str, Any] = resp.json()
            _assert_ok(payload, "category-tree")
            return payload.get("data") or []
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                sys.exit(f"❌ Дерево категорій недоступне після {MAX_RETRIES} спроб: {exc}")
            wait = RETRY_BACKOFF * attempt
            log.warning(
                "Retry %d/%d (category tree): %s — waiting %.1fs",
                attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)
    raise RuntimeError("unreachable")


def build_breadcrumb_index(tree: list[dict[str, Any]]) -> dict[str, CategoryTreeInfo]:
    """
    Сплющує дерево категорій у плоский індекс code → CategoryTreeInfo,
    де breadcrumb — повний шлях root → ... → цей вузол (uk, fallback ru).

    Індексуються ВСІ вузли (не лише листові) — кожен має власний коректний
    breadcrumb від кореня до себе.
    """
    index: dict[str, CategoryTreeInfo] = {}

    def walk(nodes: list[dict[str, Any]], ancestors: tuple[str, ...]) -> None:
        for node in nodes:
            name = _localized(node.get("name"), "uk", "ru")
            path = ancestors + ((name,) if name else ())

            code = str(node.get("code") or "").strip()
            if code:
                index[code] = CategoryTreeInfo(
                    breadcrumb = " > ".join(path),
                    slug_uk    = _localized(node.get("slug"), "uk", "ru"),
                )

            children: list[dict[str, Any]] = node.get("children") or []
            if children:
                walk(children, path)

    walk(tree, ())

    if not index:
        log.warning(
            "⚠️  Дерево категорій порожнє — breadcrumb будуватиметься "
            "з категорій товару (fallback на leaf-назву)."
        )
    else:
        log.info("🌳 Дерево категорій: %d вузлів", len(index))

    return index


# ─────────────────────────────────────────────────────────────────────
# MANUFACTURER MATCHING
# ─────────────────────────────────────────────────────────────────────
def _slug_value(raw: Any) -> str:
    """
    Normalize slug that may be a multilingual dict OR a plain string.
    Returns the first non-empty value, lowercased and stripped.
    """
    if isinstance(raw, dict):
        for v in raw.values():
            if v and isinstance(v, str):
                return v.lower().strip()
        return ""
    return str(raw).lower().strip() if raw else ""


def is_target_manufacturer(item: dict[str, Any]) -> bool:
    mfr = item.get("manufacturer")
    if not mfr or not isinstance(mfr, dict):
        return False
    return _slug_value(mfr.get("slug", "")) in TARGET_MANUFACTURERS


def _is_available(item: dict[str, Any]) -> bool:
    """Mirror of api.py ALLOWED_STATUSES filter: inStock + quickProduction."""
    return item.get("status") in ALLOWED_STATUSES


def _has_personal_usd_price(item: dict[str, Any]) -> bool:
    """Mirror of api.py spider filter: skip items without a dealer USD price."""
    for p in item.get("prices") or []:
        money = p.get("money") or {}
        if (
            p.get("type") == "personal"
            and money.get("currency") == "USD"
            and money.get("amount") is not None
        ):
            return True
    return False


# ─────────────────────────────────────────────────────────────────────
# CATEGORY EXTRACTION
# ─────────────────────────────────────────────────────────────────────
def _localized(field: Any, primary: str = "uk", fallback: str = "ru") -> str:
    """Return localized string from a multilingual dict or plain value."""
    if isinstance(field, dict):
        return (
            field.get(primary)
            or field.get(fallback)
            or next((v for v in field.values() if isinstance(v, str) and v), "")
        ) or ""
    return str(field).strip() if field else ""


def _is_excluded(code: str) -> bool:
    return code in EXCLUDED_CATEGORY_CODES


def _build_record(
    cats: list[dict[str, Any]],
    tree_index: dict[str, CategoryTreeInfo],
) -> CategoryRecord | None:
    """
    Build a CategoryRecord for a product's leaf category.

    breadcrumb / slug приходять з дерева категорій (tree_index) за кодом
    листової категорії — це повний шлях root → ... → leaf.

    Якщо код відсутній у дереві (не повинно траплятись, але про всяк
    випадок) — fallback на breadcrumb, зібраний із categories[] товару
    (може містити лише leaf, якщо LP не повернув предків).

    Returns None when the array is empty or the leaf has no code.
    """
    if not cats:
        return None

    leaf = cats[-1]
    code = str(leaf.get("code") or "").strip()
    if not code:
        return None

    leaf_name = _localized(leaf.get("name"), "uk", "ru")
    leaf_slug = _localized(leaf.get("slug"), "uk", "ru")

    tree_info = tree_index.get(code)
    if tree_info is not None and tree_info.breadcrumb:
        breadcrumb = tree_info.breadcrumb
        slug_uk    = tree_info.slug_uk or leaf_slug
    else:
        names = [
            name
            for c in cats
            if (name := _localized(c.get("name"), "uk", "ru"))
        ]
        breadcrumb = " > ".join(names) if names else leaf_name
        slug_uk    = leaf_slug
        log.debug("Код %s відсутній у дереві категорій — fallback breadcrumb: %s", code, breadcrumb)

    return CategoryRecord(
        code       = code,
        name_uk    = leaf_name,
        breadcrumb = breadcrumb,
        slug_uk    = slug_uk,
    )


def collect_categories(
    products: Iterator[dict[str, Any]],
    tree_index: dict[str, CategoryTreeInfo],
) -> dict[str, CategoryRecord]:
    """
    Stream products → collect unique leaf categories from target-manufacturer items.

    Mirrors the spider's exact filter chain (api.py → _build_item):
      1. target manufacturer
      2. status in ALLOWED_STATUSES  ← inStock + quickProduction
      3. has personal USD price
      4. not in EXCLUDED_CATEGORY_CODES

    Key   = category code (matches col 16 in the CSV).
    Value = CategoryRecord (first seen wins for a given code).
    """
    seen: dict[str, CategoryRecord] = {}
    n_total       = 0
    n_matched     = 0
    n_excluded    = 0
    n_unavailable = 0
    n_no_price    = 0

    for item in products:
        n_total += 1
        if not is_target_manufacturer(item):
            continue

        if not _is_available(item):
            n_unavailable += 1
            continue

        if not _has_personal_usd_price(item):
            n_no_price += 1
            continue

        n_matched += 1
        raw_cats: list[dict[str, Any]] = item.get("categories") or []
        record = _build_record(raw_cats, tree_index)

        if record is None:
            log.debug("Пропуск: немає валідної категорії у товарі %s", item.get("id"))
            continue

        if _is_excluded(record.code):
            n_excluded += 1
            continue

        if record.code not in seen:
            seen[record.code] = record
            log.debug("+ категорія [%s] %s", record.code, record.breadcrumb)

    log.info(
        "✅ Товарів: %d | відповідає виробникам: %d "
        "| недоступних (не inStock/quickProduction): %d | без dealer-ціни: %d "
        "| унікальних категорій: %d | виключено: %d",
        n_total, n_matched, n_unavailable, n_no_price, len(seen), n_excluded,
    )
    return seen


# ─────────────────────────────────────────────────────────────────────
# CSV ROW BUILDER
# ─────────────────────────────────────────────────────────────────────
def _category_link(rec: CategoryRecord, frontend_url: str) -> str:
    """Construct a human-readable frontend link for the category, if slug available."""
    if not frontend_url or not rec.slug_uk:
        return ""
    return f"{frontend_url.rstrip('/')}/catalog/{rec.slug_uk}/"


def _build_row(
    row_num: int,
    rec: CategoryRecord,
    channel: str,
    frontend_url: str,
) -> list[str]:
    """
    One CSV data row for a NEW (category × channel) pair.
    Only auto-filled columns are set; everything else stays empty
    for manual fill or lp_map_categories.py.
    """
    row: list[str] = [""] * len(CSV_HEADER)

    row[_COL_IDX["№"]]                          = str(row_num)
    row[_COL_IDX["Линк категории поставщика"]]   = _category_link(rec, frontend_url)
    row[_COL_IDX["channel"]]                     = channel
    row[_COL_IDX["category id"]]                 = rec.code
    row[_COL_IDX["Назва у постачальника"]]       = rec.breadcrumb

    return row


# ─────────────────────────────────────────────────────────────────────
# CSV MERGE / EXPORT  (idempotent — append-only)
# ─────────────────────────────────────────────────────────────────────
def _read_existing(path: Path) -> ExistingCsvData:
    """
    Прочитати наявний lp_category.csv (якщо є).

    Рядки з category id у EXCLUDED_CATEGORY_CODES відкидаються тут —
    саме так раніше записані виключені категорії (напр. Уцінка)
    зникають із файлу при наступному запуску.
    """
    if not path.exists():
        return ExistingCsvData(rows=[], index={}, max_row_num=0)

    width = len(CSV_HEADER)
    rows: list[list[str]] = []
    index: dict[tuple[str, str], int] = {}
    max_row_num = 0
    n_excluded  = 0

    with path.open("r", newline="", encoding=CSV_ENCODING) as fh:
        reader = csv.reader(fh, delimiter=CSV_DELIMITER)

        header = next(reader, None)
        if header is not None and header != CSV_HEADER:
            log.warning(
                "⚠️  Заголовок %s відрізняється від CSV_HEADER — "
                "продовжуємо, колонки мапляться за позицією.",
                path.name,
            )

        for raw in reader:
            if not raw:
                continue

            row = (raw + [""] * width)[:width]

            code = row[_COL_IDX["category id"]].strip()
            if _is_excluded(code):
                n_excluded += 1
                continue

            channel = row[_COL_IDX["channel"]].strip()
            if code and channel:
                index[(channel, code)] = len(rows)

            try:
                max_row_num = max(max_row_num, int(row[_COL_IDX["№"]].strip()))
            except ValueError:
                pass

            rows.append(row)

    if n_excluded:
        log.info(
            "🗑️  Знайдено %d рядків виключених категорій у файлі — буде очищено при записі.",
            n_excluded,
        )

    return ExistingCsvData(rows=rows, index=index, max_row_num=max_row_num, n_excluded=n_excluded)


def merge_and_export_csv(
    categories: dict[str, CategoryRecord],
    output_path: Path,
    frontend_url: str,
) -> None:
    """
    Ідемпотентне об'єднання categories з output_path.

    Стратегія запису (три сценарії):
      1. Файл відсутній → пишемо з нуля (заголовок + нові рядки).
      2. Файл є + знайдено рядки виключених категорій → одноразове очищення:
         перезапис без виключених рядків + дописування нових у кінець.
      3. Файл є + жодних виключених рядків → тільки дописування нових рядків
         (режим 'a'). Існуючі рядки НЕ змінюються, ручні правки збережені.

    Нові рядки — ті, яких немає у файлі за ключем (channel, category id).
    EXCLUDED_CATEGORY_CODES ніколи не потрапляють до нових рядків.
    Нові рядки сортуються за breadcrumb (site → prom для кожної категорії).
    """
    existing = _read_existing(output_path)

    # Collect (record, channel) pairs that are not yet in the file
    new_pairs: list[tuple[CategoryRecord, str]] = [
        (rec, channel)
        for rec in categories.values()
        for channel in CHANNELS
        if (channel, rec.code) not in existing.index
    ]
    new_pairs.sort(key=lambda p: (p[0].breadcrumb.casefold(), CHANNELS.index(p[1])))

    next_num = existing.max_row_num
    new_rows: list[list[str]] = []
    for rec, channel in new_pairs:
        next_num += 1
        new_rows.append(_build_row(next_num, rec, channel, frontend_url))

    output_path.parent.mkdir(parents=True, exist_ok=True)

    file_is_new   = not output_path.exists() or output_path.stat().st_size == 0
    needs_cleanup = existing.n_excluded > 0

    if file_is_new:
        # ── Case 1: brand-new file ─────────────────────────────────────────
        with output_path.open("w", newline="", encoding=CSV_ENCODING) as fh:
            writer = csv.writer(fh, delimiter=CSV_DELIMITER, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(CSV_HEADER)
            writer.writerows(new_rows)
        log.info(
            "💾 Новий файл %s | записано рядків: %d",
            output_path, len(new_rows),
        )

    elif needs_cleanup:
        # ── Case 2: one-time cleanup of excluded rows ──────────────────────
        with output_path.open("w", newline="", encoding=CSV_ENCODING) as fh:
            writer = csv.writer(fh, delimiter=CSV_DELIMITER, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(CSV_HEADER)
            writer.writerows(existing.rows)   # already filtered (no excluded codes)
            writer.writerows(new_rows)
        log.info(
            "🧹 %s | очищено виключених рядків: %d | додано нових: %d | всього: %d",
            output_path,
            existing.n_excluded,
            len(new_rows),
            len(existing.rows) + len(new_rows),
        )

    else:
        # ── Case 3: normal re-run — append only ───────────────────────────
        if new_rows:
            with output_path.open("a", newline="", encoding=CSV_ENCODING) as fh:
                writer = csv.writer(fh, delimiter=CSV_DELIMITER, quoting=csv.QUOTE_MINIMAL)
                writer.writerows(new_rows)
            log.info(
                "💾 %s | дописано нових рядків: %d",
                output_path, len(new_rows),
            )
        else:
            log.info("✔️  %s | нових категорій немає — файл не змінено", output_path)


# ─────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────
def main() -> None:
    load_dotenv(_ENV_FILE)

    token        = os.getenv("LP_API_TOKEN",    "").strip()
    base_url     = os.getenv("LP_API_BASE_URL", "").strip()
    # LP_FRONTEND_URL is optional; used only to build col-1 links.
    # Example: https://b2b.logicpower.ua
    # If absent, col-1 stays empty for any newly added rows.
    frontend_url = os.getenv("LP_FRONTEND_URL", "").strip()

    if not token:
        sys.exit("❌ LP_API_TOKEN не знайдено в suppliers/.env")
    if not base_url:
        sys.exit("❌ LP_API_BASE_URL не знайдено в suppliers/.env")

    mfr_label = " + ".join(sorted(m.capitalize() for m in TARGET_MANUFACTURERS))

    log.info("🔑 Token:      %s***", token[:8])
    log.info("🌐 API URL:    %s", base_url)
    log.info("🏭 Виробники: %s", mfr_label)
    log.info("📄 Output:     %s", _OUTPUT_CSV)

    session = build_session(token)

    tree       = fetch_category_tree(session, base_url)
    tree_index = build_breadcrumb_index(tree)

    products   = iter_products(session, base_url)
    categories = collect_categories(products, tree_index)

    if not categories:
        log.warning(
            "⚠️  Категорій не знайдено. "
            "Перевірте TARGET_MANUFACTURERS або відповідь API."
        )
        sys.exit(1)

    merge_and_export_csv(categories, _OUTPUT_CSV, frontend_url)

    log.info(
        "🎉 Готово. Наступний крок:\n"
        "   python scripts/lp_map_categories.py"
    )


if __name__ == "__main__":
    main()
