"""
services/rozetka_stop_brand_service.py
---------------------------------------
Фільтрує Rozetka-фід за публічним Google Sheets списком стоп-брендів.

Таблиця: «Стоп-категорії та стоп-бренди маркетплейс» → лист gid=672087803.
Сервіс знаходить стовпець «Назва бренду / ТМ» за заголовком (без прив'язки
до позиції) і зчитує всі непорожні клітинки під ним.

Використання в generate_rozetka_feed.py:
    from services.rozetka_stop_brand_service import filter_stop_brand_offers
    updated_xml = filter_stop_brand_offers(updated_xml)
"""

from __future__ import annotations

import csv
import html
import re
from collections import Counter
from io import StringIO
from typing import Final, Iterable

import requests
from requests import RequestException

# ---------------------------------------------------------------------------
# Google Sheets config
# Published spreadsheet uses e/ path — URL differs from regular sheets.
# ---------------------------------------------------------------------------

_PUBLISHED_ID: Final[str] = (
    "2PACX-1vQqHOjuMG8fd9FMF6__c9kEE6IoVvYEOKmysmJpMDVuNj-XdsAkmQp1AR34pQ0Dqg"
)
_SHEET_GID: Final[str] = "672087803"

# Multiple URL patterns for the same published sheet — tried in order as fallbacks.
# Published sheets expose only the pub?output=csv endpoint (no /export, no gviz by default).
STOP_BRANDS_CSV_URL: Final[str] = (
    f"https://docs.google.com/spreadsheets/d/e/{_PUBLISHED_ID}"
    f"/pub?gid={_SHEET_GID}&single=true&output=csv"
)
_STOP_BRANDS_CSV_URLS: Final[tuple[str, ...]] = (
    STOP_BRANDS_CSV_URL,
    # Alternative pub format (some older GSheets versions respond to this)
    (
        f"https://docs.google.com/spreadsheets/d/e/{_PUBLISHED_ID}"
        f"/pub?gid={_SHEET_GID}&output=csv"
    ),
)

_REQUEST_TIMEOUT: Final[int] = 30

# ---------------------------------------------------------------------------
# Header variants that identify the "brand name" column in the sheet.
# Matched after casefold + collapse of -, _, whitespace variants.
# ---------------------------------------------------------------------------

_BRAND_COL_HEADERS: Final[frozenset[str]] = frozenset({
    "назва бренду",
    "назва бренду / тм",
    "назва бренду/тм",
    "назва тм",
    "бренд",
    "бренди",
    "торгова марка",
    "тм",
    "brand",
    "brands",
    "stop brand",
    "stop brands",
})

# Prom <param> names that represent the brand field inside an <offer>.
_PROM_BRAND_PARAM_NAMES: Final[frozenset[str]] = frozenset({
    "brand",
    "бренд",
    "виробник",
    "производитель",
    "торгова марка",
    "торговая марка",
    "марка",
})

# ---------------------------------------------------------------------------
# Compiled regexes — module-level, compiled once.
# ---------------------------------------------------------------------------

_OFFER_RE: Final[re.Pattern[str]] = re.compile(
    r'[ \t]*<offer\s+id="([^"]+)"([^>]*)>(.*?)</offer>[ \t]*\n?',
    re.DOTALL,
)
_VENDOR_RE: Final[re.Pattern[str]] = re.compile(
    r"<vendor>(.*?)</vendor>",
    re.DOTALL,
)
_PROM_PARAM_RE: Final[re.Pattern[str]] = re.compile(
    r'<param\b[^>]*\bname="([^"]+)"[^>]*>(.*?)</param>',
    re.DOTALL,
)
_CDATA_RE: Final[re.Pattern[str]] = re.compile(
    r'<!\[CDATA\[(.*?)\]\]>',
    re.DOTALL,
)
_HTML_TAG_RE: Final[re.Pattern[str]] = re.compile(r"<[^>]+>")

# Splits composite brand values like "Samsung / LG", "Bosch, Siemens", "A + B".
_BRAND_SPLIT_RE: Final[re.Pattern[str]] = re.compile(
    r"\s*(?:[,;/|]|\s+\+\s+)\s*"
)


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def _clean_text(value: str) -> str:
    """Strip CDATA wrappers, HTML tags, HTML entities, and collapse whitespace."""
    value = _CDATA_RE.sub(lambda m: m.group(1), value)
    value = _HTML_TAG_RE.sub("", value)
    value = html.unescape(value)
    value = value.replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def _normalize_brand(value: str) -> str:
    """Casefold + clean — used as dict key for brand matching."""
    return _clean_text(value).casefold()


def _header_key(value: str) -> str:
    """Normalize a header cell for comparison against _BRAND_COL_HEADERS."""
    value = _normalize_brand(value)
    value = value.replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", value).strip()


def _looks_like_html(text: str) -> bool:
    head = text.lstrip()[:200].casefold()
    return head.startswith("<!doctype html") or head.startswith("<html")


# ---------------------------------------------------------------------------
# CSV download & parsing
# ---------------------------------------------------------------------------

def _download_csv(url: str) -> str:
    response = requests.get(url, timeout=_REQUEST_TIMEOUT)
    response.raise_for_status()
    csv_text = response.content.decode("utf-8-sig")
    if _looks_like_html(csv_text):
        raise RuntimeError("Google Sheets повернув HTML замість CSV")
    return csv_text


def _rows_from_csv(csv_text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for row in csv.reader(StringIO(csv_text)):
        cleaned = [_clean_text(cell) for cell in row]
        if any(cleaned):
            rows.append(cleaned)
    return rows


# ---------------------------------------------------------------------------
# Column detection & brand extraction
# ---------------------------------------------------------------------------

def _find_brand_column(rows: list[list[str]]) -> tuple[int, int] | None:
    """
    Returns (header_row_idx, col_idx) of the "Назва бренду / ТМ" column.

    Searches the first 10 rows to allow for title/subtitle rows above the header.
    Returns None if the column is not found.
    """
    for row_idx, row in enumerate(rows[:10]):
        for col_idx, cell in enumerate(row):
            if _header_key(cell) in _BRAND_COL_HEADERS:
                return row_idx, col_idx
    return None


def _iter_brand_values(
    rows: list[list[str]],
    header_row_idx: int,
    col_idx: int,
) -> Iterable[str]:
    """
    Yields non-empty brand strings from col_idx, starting after header_row_idx.

    Rows above the header (title/subtitle rows) are intentionally skipped
    so that they are never mistaken for brand names.
    """
    for row in rows[header_row_idx + 1 :]:
        if col_idx >= len(row):
            continue
        value = row[col_idx]
        if value:
            yield value


# ---------------------------------------------------------------------------
# Public: load stop brands
# ---------------------------------------------------------------------------

def load_stop_brands(url: str | None = None) -> frozenset[str]:
    """
    Downloads stop-brand names from the public Google Sheet CSV export.

    Args:
        url: Override URL for testing; production uses _STOP_BRANDS_CSV_URLS.

    Returns:
        Frozenset of raw (pre-normalization) brand name strings.

    Raises:
        RuntimeError: If all URLs fail or the brand column is not found.
    """
    errors: list[str] = []
    for csv_url in ((url,) if url else _STOP_BRANDS_CSV_URLS):
        try:
            csv_text = _download_csv(csv_url)
            rows = _rows_from_csv(csv_text)
        except (RequestException, RuntimeError, UnicodeDecodeError) as exc:
            errors.append(f"{csv_url}: {exc}")
            continue

        col_result = _find_brand_column(rows)
        if col_result is None:
            errors.append(
                f"{csv_url}: стовпець «Назва бренду / ТМ» не знайдено "
                f"(перевірте заголовки: {[r[:5] for r in rows[:3]]})"
            )
            continue

        header_row_idx, col_idx = col_result
        brands = frozenset(_iter_brand_values(rows, header_row_idx, col_idx))
        if brands:
            print(f"🚫 Rozetka стоп-бренди: завантажено {len(brands)}")
            return brands

        errors.append(f"{csv_url}: список порожній або нечитабельний")

    raise RuntimeError(
        "Не вдалося завантажити стоп-бренди Rozetka:\n" + "\n".join(errors)
    )


# ---------------------------------------------------------------------------
# Brand matching helpers
# ---------------------------------------------------------------------------

def _stop_brand_index(stop_brands: Iterable[str]) -> dict[str, str]:
    """
    Builds a {normalized_brand: display_brand} lookup dict.

    Uses setdefault to keep the first encountered display form on duplicates.
    """
    index: dict[str, str] = {}
    for brand in stop_brands:
        norm = _normalize_brand(brand)
        if norm:
            index.setdefault(norm, _clean_text(brand))
    return index


def _brand_candidates(body: str) -> list[str]:
    """
    Extracts candidate brand strings from a single <offer> body:
      1. <vendor> tag value (primary source in Rozetka/Prom feeds)
      2. <param name="Бренд|brand|..."> values (secondary / fallback)

    Returns a list of non-empty cleaned strings (preserves order: vendor first).
    """
    candidates: list[str] = []

    vendor_match = _VENDOR_RE.search(body)
    if vendor_match:
        candidates.append(_clean_text(vendor_match.group(1)))

    for param_match in _PROM_PARAM_RE.finditer(body):
        param_name = _normalize_brand(param_match.group(1))
        if param_name in _PROM_BRAND_PARAM_NAMES:
            candidates.append(_clean_text(param_match.group(2)))

    return [c for c in candidates if c]


def _matched_stop_brand(
    candidates: Iterable[str],
    stop_index: dict[str, str],
) -> str | None:
    """
    Returns the display name of the first stop brand matched in candidates,
    or None if no match.

    Each candidate is checked both whole and split on separators (/, ,, ;, |, +)
    to handle composite brand strings like "Bosch / Siemens".
    """
    for candidate in candidates:
        for token in (candidate, *_BRAND_SPLIT_RE.split(candidate)):
            norm = _normalize_brand(token)
            if norm in stop_index:
                return stop_index[norm]
    return None


# ---------------------------------------------------------------------------
# Public: filter XML feed
# ---------------------------------------------------------------------------

def filter_stop_brand_offers(
    xml: str,
    stop_brands: Iterable[str] | None = None,
) -> str:
    """
    Removes <offer> elements whose vendor/brand matches the stop-brand list.

    Args:
        xml:         Full XML feed string.
        stop_brands: Override brand list for tests; production loads from Google Sheet.

    Returns:
        XML with matching offers removed (empty string per removed offer).

    Raises:
        RuntimeError: If the stop-brand list is empty after normalization.
    """
    raw_stop_brands = (
        load_stop_brands() if stop_brands is None else frozenset(stop_brands)
    )
    stop_index = _stop_brand_index(raw_stop_brands)
    if not stop_index:
        raise RuntimeError("Список стоп-брендів Rozetka порожній після нормалізації")

    removed_by_brand: Counter[str] = Counter()

    def _on_offer(m: re.Match[str]) -> str:
        body = m.group(3)
        matched_brand = _matched_stop_brand(_brand_candidates(body), stop_index)
        if matched_brand is None:
            return m.group(0)
        removed_by_brand[matched_brand] += 1
        return ""

    filtered_xml = _OFFER_RE.sub(_on_offer, xml)

    removed_total = sum(removed_by_brand.values())
    if removed_total:
        brands_summary = ", ".join(
            f"{brand} ({count})" for brand, count in removed_by_brand.most_common()
        )
        print(
            f"🚫 Rozetka стоп-бренди: видалено {removed_total} товарів "
            f"| {brands_summary}"
        )
    else:
        print("🚫 Rozetka стоп-бренди: збігів у фіді немає")

    return filtered_xml
