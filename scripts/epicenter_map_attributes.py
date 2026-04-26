"""
epicenter_map_attributes.py
---------------------------
Зіставляє атрибути Epicenter з параметрами Prom (фаззі-матчинг ≥ 80%).
Читає epicenter_mappings.xlsx, записує prom_param_name, підсвічує червоним обов'язкові поля.

Запуск:
    python scripts/epicenter_map_attributes.py

Алгоритм:
1. Download Prom XML feed and index param names by categoryId.
2. Read sheet "Маппінг": for each row with prom_category_id + epicenter_category_id.
3. Find matching rows in sheet "Сети атрибутів" by set_code == epicenter_category_id.
4. Fuzzy-match each attr_name_uk against unique prom param names (threshold ≥ 80%).
5. Write best-matching prom param name into column prom_param_name.
6. Highlight red: isRequired=TRUE + empty prom_param_name + category is mapped.
7. Save the workbook.
"""

import re
from collections import defaultdict
from pathlib import Path
from xml.etree import ElementTree as ET

import requests
import openpyxl
from openpyxl.styles import PatternFill
from rapidfuzz import fuzz

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
FEED_URL = (
    "https://oniks.org.ua/rozetka_feed.xml?rozetka_hash_tag=33ec12f81c283cc0524764696220b10c"
    "&product_ids=&label_ids=&languages=uk%2Cru"
    "&group_ids=2221523%2C2222437%2C2222561%2C2234751%2C4320349%2C4321341%2C4325742%2C4325743"
    "%2C4328775%2C4331550%2C4339717%2C4551903%2C8950007%2C8950011%2C10015559%2C16703618"
    "%2C17012111%2C17012456%2C22818554%2C23295147%2C45720479%2C72575633%2C83889367%2C90718784"
    "%2C90906797%2C90997501%2C90997677%2C90997694%2C91056839%2C127351905%2C127351912"
    "%2C127351948%2C127351950%2C127351973%2C127628160%2C127628166%2C127628170%2C127628173"
    "%2C127628176%2C139094517%2C139094704%2C139094708%2C144038788%2C144038790%2C144038804"
    "%2C151114178%2C152084397%2C152084437%2C152084460%2C152084594%2C152084669%2C152084678"
    "%2C152084703%2C152086699%2C152088176%2C152088624%2C152090354%2C152090439%2C152090654"
    "%2C152090742%2C152090999%2C152091016%2C152091894%2C152092523%2C152092600%2C152092613"
    "%2C152092625%2C152104228%2C152104243%2C152133169%2C152133408%2C152133464%2C152133483"
    "%2C152135823%2C152195979%2C152196244%2C152197115%2C152197317%2C152197474%2C152206635"
    "%2C152207998%2C152208073%2C152208101%2C152208132%2C152208469%2C152208563%2C152208591"
    "%2C152208632%2C152481182%2C152481185%2C152481192%2C152481294%2C152483771"
    "&nested_group_ids=4321341%2C4325742%2C4325743%2C4328775%2C4331550%2C4339717%2C4551903"
    "%2C8950007%2C8950011%2C16703618%2C17012111%2C17012456%2C22818554%2C23295147%2C45720479"
    "%2C72575633%2C83889367%2C90718784%2C90906797%2C90997501%2C90997677%2C90997694%2C91056839"
    "%2C127351912%2C127351948%2C127351950%2C127351973%2C127628160%2C127628166%2C127628170"
    "%2C127628173%2C127628176%2C139094704%2C139094708%2C144038788%2C144038790%2C144038804"
    "%2C151114178%2C152084397%2C152084437%2C152084460%2C152084594%2C152084669%2C152084678"
    "%2C152084703%2C152086699%2C152088176%2C152088624%2C152090354%2C152090439%2C152090654"
    "%2C152090742%2C152090999%2C152091016%2C152091894%2C152092523%2C152092600%2C152092613"
    "%2C152092625%2C152104243%2C152133169%2C152133408%2C152133464%2C152133483%2C152135823"
    "%2C152195979%2C152196244%2C152197115%2C152197317%2C152197474%2C152206635%2C152207998"
    "%2C152208073%2C152208101%2C152208132%2C152208469%2C152208563%2C152208591%2C152208632"
    "%2C152481182%2C152481185%2C152481192%2C152481294%2C152483771"
)

XLSX_PATH = Path(__file__).parents[1] / "data" / "markets" / "epicenter_mappings.xlsx"

SHEET_MAPPING = "Маппінг"
SHEET_ATTRS   = "Сети атрибутів"

# Columns – Маппінг
MAP_COL_PROM_CAT = "prom_category_id"
MAP_COL_EPI_CAT  = "epicenter_category_id"

# Columns – Сети атрибутів
ATTR_COL_SET_CODE   = "set_code"
ATTR_COL_ATTR_NAME  = "attr_name_uk"
ATTR_COL_PROM_PARAM = "prom_param_name"
ATTR_COL_IS_REQUIRED = "isRequired"

MATCH_THRESHOLD = 80  # percent

# Примусові маппінги: attr_name_uk → prom_param_name (мають пріоритет над fuzzy)
HARD_MAPPINGS: dict[str, str] = {
    "Глибина":                           "Длина",
    "Одиниця виміру та кількість":        "шт.",
    "Мінімальна кратність товару":        "1",
    "Бренд":                             "Компанія-виробник",
}

RED_FILL = PatternFill("solid", start_color="FF9999", end_color="FF9999")
NO_FILL  = PatternFill(fill_type=None)


# ──────────────────────────────────────────────
# Step 1 – Download & parse XML feed
# ──────────────────────────────────────────────

def download_feed(url: str) -> ET.Element:
    print("Downloading feed…")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content) / 1024:.0f} KB")
    return ET.fromstring(resp.content)


def build_category_params(root: ET.Element) -> dict[str, set[str]]:
    """
    Returns { category_id_str: {param_name, ...} }
    Handles both:
      <offer><categoryId>513</categoryId><param name="Вес">…</param></offer>
      and namespaced variants.
    """
    params_by_cat: dict[str, set[str]] = defaultdict(set)

    offers = root.findall(".//{*}offer") or root.findall(".//offer")
    if not offers:
        offers = root.findall(".//{*}item") or root.findall(".//item")

    for offer in offers:
        cat_el = offer.find("{*}categoryId") or offer.find("categoryId")
        if cat_el is None or not (cat_el.text or "").strip():
            cat_el = offer.find("{http://base.google.com/ns/1.0}google_product_category")
        if cat_el is None:
            continue
        cat_id = cat_el.text.strip()

        for param in offer.findall("{*}param") or offer.findall("param") or []:
            name = param.get("name", "").strip()
            if name:
                params_by_cat[cat_id].add(name)

    print(f"  Indexed {len(params_by_cat)} categories from feed.")
    return dict(params_by_cat)


# ──────────────────────────────────────────────
# Step 2 – Fuzzy matching helpers
# ──────────────────────────────────────────────

def normalize(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    return re.sub(r"\s+", " ", text).strip()


def best_prom_match(epi_attr: str, prom_params: set[str]) -> str | None:
    """
    Find the prom param name that best matches epi_attr.
    Returns the original prom param string or None if score < MATCH_THRESHOLD.
    """
    best_score = 0.0
    best_name: str | None = None

    n_epi = normalize(epi_attr)

    for prom in prom_params:
        n_prom = normalize(prom)
        score = max(
            fuzz.ratio(n_epi, n_prom),
            fuzz.partial_ratio(n_epi, n_prom),
            fuzz.token_sort_ratio(n_epi, n_prom),
        )
        if score > best_score:
            best_score = score
            best_name = prom

    if best_score >= MATCH_THRESHOLD:
        return best_name
    return None


# ──────────────────────────────────────────────
# Step 3 – Read xlsx helpers
# ──────────────────────────────────────────────

def get_header_map(ws) -> dict[str, int]:
    """Returns {column_name: 1-based column index} from first row."""
    return {
        str(cell.value).strip(): idx + 1
        for idx, cell in enumerate(ws[1])
        if cell.value is not None
    }


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main() -> None:
    if not XLSX_PATH.exists():
        raise FileNotFoundError(f"File not found: {XLSX_PATH}")

    # ── 1. Download feed ──────────────────────
    root = download_feed(FEED_URL)
    cat_params = build_category_params(root)

    # ── 2. Open workbook ──────────────────────
    wb = openpyxl.load_workbook(XLSX_PATH)

    for sheet in (SHEET_MAPPING, SHEET_ATTRS):
        if sheet not in wb.sheetnames:
            raise ValueError(f"Sheet '{sheet}' not found. Available: {wb.sheetnames}")

    ws_map  = wb[SHEET_MAPPING]
    ws_attr = wb[SHEET_ATTRS]

    map_hdrs  = get_header_map(ws_map)
    attr_hdrs = get_header_map(ws_attr)

    for col, sheet_name, hdrs in [
        (MAP_COL_PROM_CAT,    SHEET_MAPPING, map_hdrs),
        (MAP_COL_EPI_CAT,     SHEET_MAPPING, map_hdrs),
        (ATTR_COL_SET_CODE,   SHEET_ATTRS,   attr_hdrs),
        (ATTR_COL_ATTR_NAME,  SHEET_ATTRS,   attr_hdrs),
        (ATTR_COL_PROM_PARAM, SHEET_ATTRS,   attr_hdrs),
        (ATTR_COL_IS_REQUIRED, SHEET_ATTRS,  attr_hdrs),
    ]:
        if col not in hdrs:
            raise ValueError(
                f"Column '{col}' not found in sheet '{sheet_name}'.\n"
                f"Found: {list(hdrs.keys())}"
            )

    col_prom_cat    = map_hdrs[MAP_COL_PROM_CAT]
    col_epi_cat     = map_hdrs[MAP_COL_EPI_CAT]
    col_set_code    = attr_hdrs[ATTR_COL_SET_CODE]
    col_attr_name   = attr_hdrs[ATTR_COL_ATTR_NAME]
    col_prom_param  = attr_hdrs[ATTR_COL_PROM_PARAM]
    col_is_required = attr_hdrs[ATTR_COL_IS_REQUIRED]

    # ── 3. Build index: set_code → [(row_idx, attr_name_uk, is_required)] ──
    attr_index: dict[str, list[tuple[int, str, bool]]] = defaultdict(list)
    for row_idx in range(2, ws_attr.max_row + 1):
        set_code  = str(ws_attr.cell(row_idx, col_set_code).value or "").strip()
        attr_name = str(ws_attr.cell(row_idx, col_attr_name).value or "").strip()
        is_required = str(ws_attr.cell(row_idx, col_is_required).value or "").strip().upper() == "TRUE"
        if set_code and attr_name:
            attr_index[set_code].append((row_idx, attr_name, is_required))

    print(f"Loaded {sum(len(v) for v in attr_index.values())} attribute rows "
          f"across {len(attr_index)} set_codes.\n")

    # ── 4. Fuzzy-match and write prom_param_name ──
    total_written  = 0
    total_skipped  = 0
    processed_epi_cats: set[str] = set()  # категорії що обробили

    for map_row in range(2, ws_map.max_row + 1):
        prom_cat = str(ws_map.cell(map_row, col_prom_cat).value or "").strip()
        epi_cat  = str(ws_map.cell(map_row, col_epi_cat).value or "").strip()

        if not prom_cat or not epi_cat:
            continue

        prom_params = cat_params.get(prom_cat, set())
        if not prom_params:
            print(f"  [row {map_row}] prom_cat={prom_cat} — no params in feed, skipping")
            continue

        epi_attrs = attr_index.get(epi_cat, [])
        if not epi_attrs:
            print(f"  [row {map_row}] epi_cat={epi_cat} — not found in Сети атрибутів, skipping")
            continue

        print(f"[row {map_row}] prom_cat={prom_cat} → epi_cat={epi_cat} "
              f"| {len(prom_params)} prom params, {len(epi_attrs)} epi attrs")

        processed_epi_cats.add(epi_cat)

        for attr_row, attr_name, is_required in epi_attrs:
            if not is_required:
                continue  # заповнюємо тільки обов'язкові

            cell = ws_attr.cell(attr_row, col_prom_param)
            current_val = str(cell.value or "").strip()
            if current_val:
                # Вже заповнено (вручну або попереднім запуском) — не перезаписуємо
                total_skipped += 1
                continue

            match = HARD_MAPPINGS.get(attr_name) or best_prom_match(attr_name, prom_params)
            if match:
                cell.value = match
                source = "hardcoded" if attr_name in HARD_MAPPINGS else "fuzzy"
                print(f"    ✓ [{source}] '{attr_name}' → '{match}'")
                total_written += 1
            else:
                # Fuzzy не знайшов збіг — клітинку не чіпаємо
                total_skipped += 1

    # ── 5. Червона підсвітка: isRequired + порожньо + зіставлена категорія ──
    red_count = 0
    for set_code, attrs in attr_index.items():
        if set_code not in processed_epi_cats:
            continue
        for attr_row, attr_name, is_required in attrs:
            if not is_required:
                continue
            prom_cell   = ws_attr.cell(attr_row, col_prom_param)
            prom_filled = bool(str(prom_cell.value or "").strip())
            if not prom_filled:
                prom_cell.fill = RED_FILL
                red_count += 1
            else:
                prom_cell.fill = NO_FILL  # знімаємо червоний якщо вже заповнили

    wb.save(XLSX_PATH)
    print(f"\nDone. Written: {total_written} | No match: {total_skipped} | Red: {red_count}")
    print(f"Saved → {XLSX_PATH}")


if __name__ == "__main__":
    main()
