"""
services/epicenter_stop_brand_service.py
----------------------------------------
Фільтрує Epicenter-фід за публічним Google Sheets списком стоп-брендів.

Таблиця може бути як одноколонковою, так і сіткою брендів по літерах.
Сервіс читає всі непорожні клітинки й відкидає службові заголовки.
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

_SPREADSHEET_ID: Final[str] = "1i6_zYMHzm1L6tQIPXrnLnmM5EKqJs4vt6mfjRihHkYU"
_SHEET_GID: Final[str] = "0"
STOP_BRANDS_CSV_URL: Final[str] = (
    f"https://docs.google.com/spreadsheets/d/{_SPREADSHEET_ID}/export"
    f"?format=csv&gid={_SHEET_GID}"
)
_STOP_BRANDS_CSV_URLS: Final[tuple[str, ...]] = (
    STOP_BRANDS_CSV_URL,
    (
        f"https://docs.google.com/spreadsheets/d/{_SPREADSHEET_ID}/gviz/tq"
        f"?tqx=out:csv&gid={_SHEET_GID}"
    ),
    (
        f"https://docs.google.com/spreadsheets/d/{_SPREADSHEET_ID}/pub"
        f"?gid={_SHEET_GID}&single=true&output=csv"
    ),
)

_REQUEST_TIMEOUT: Final[int] = 30
_OFFER_RE: Final[re.Pattern[str]] = re.compile(
    r'[ \t]*<offer\s+id="([^"]+)"([^>]*)>(.*?)</offer>[ \t]*\n?',
    re.DOTALL,
)
_VENDOR_RE: Final[re.Pattern[str]] = re.compile(r"<vendor>(.*?)</vendor>", re.DOTALL)
_PROM_PARAM_RE: Final[re.Pattern[str]] = re.compile(
    r'<param\b[^>]*\bname="([^"]+)"[^>]*>(.*?)</param>',
    re.DOTALL,
)
_CDATA_RE: Final[re.Pattern[str]] = re.compile(r'<!\[CDATA\[(.*?)\]\]>', re.DOTALL)
_HTML_TAG_RE: Final[re.Pattern[str]] = re.compile(r"<[^>]+>")

_BRAND_HEADER_NAMES: Final[frozenset[str]] = frozenset({
    "brand",
    "brands",
    "stop brand",
    "stop brands",
    "stop_brand",
    "stop_brands",
    "бренд",
    "бренди",
    "бренды",
    "стоп бренд",
    "стоп бренди",
    "стоп бренды",
    "стоп-бренд",
    "стоп-бренди",
    "стоп-бренды",
    "виробник",
    "производитель",
    "vendor",
})
_PROM_BRAND_PARAM_NAMES: Final[frozenset[str]] = frozenset({
    "brand",
    "бренд",
    "виробник",
    "производитель",
    "торгова марка",
    "торговая марка",
    "марка",
})
_BRAND_SPLIT_RE: Final[re.Pattern[str]] = re.compile(r"\s*(?:[,;/|]|\s+\+\s+)\s*")


def _clean_text(value: str) -> str:
    value = _CDATA_RE.sub(lambda m: m.group(1), value)
    value = _HTML_TAG_RE.sub("", value)
    value = html.unescape(value)
    value = value.replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def _normalize_brand(value: str) -> str:
    return _clean_text(value).casefold()


def _header_key(value: str) -> str:
    value = _normalize_brand(value).replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", value).strip()


def _is_header_value(value: str) -> bool:
    return _header_key(value) in _BRAND_HEADER_NAMES


def _rows_from_csv(csv_text: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for row in csv.reader(StringIO(csv_text)):
        cleaned = [_clean_text(cell) for cell in row]
        if any(cleaned):
            rows.append(cleaned)
    return rows


def _looks_like_html(text: str) -> bool:
    head = text.lstrip()[:200].casefold()
    return head.startswith("<!doctype html") or head.startswith("<html")


def _is_brand_cell(value: str) -> bool:
    key = _header_key(value)
    if not key:
        return False
    if key in _BRAND_HEADER_NAMES:
        return False
    if key.startswith("нижче наведено") or "маркетплейс епіцентр" in key:
        return False
    # Alphabet row in the source sheet: A, B, C ... / А, Б, Т ...
    if len(key) == 1 and key.isalpha():
        return False
    return True


def _iter_brand_cells(rows: list[list[str]]) -> Iterable[str]:
    for row in rows:
        for value in row:
            if _is_brand_cell(value):
                yield value


def _download_csv(url: str) -> str:
    response = requests.get(url, timeout=_REQUEST_TIMEOUT)
    response.raise_for_status()

    csv_text = response.content.decode("utf-8-sig")
    if _looks_like_html(csv_text):
        raise RuntimeError("Google Sheets повернув HTML замість CSV")
    return csv_text


def load_stop_brands(url: str | None = None) -> frozenset[str]:
    """Loads raw stop-brand names from the public Google Sheet CSV export."""
    errors: list[str] = []
    for csv_url in ((url,) if url else _STOP_BRANDS_CSV_URLS):
        try:
            brands = frozenset(_iter_brand_cells(_rows_from_csv(_download_csv(csv_url))))
        except (RequestException, RuntimeError, UnicodeDecodeError) as exc:
            errors.append(f"{csv_url}: {exc}")
            continue

        if brands:
            print(f"🚫 Epicenter стоп-бренди: завантажено {len(brands)}")
            return brands

        errors.append(f"{csv_url}: список порожній або нечитабельний")

    raise RuntimeError(
        "Не вдалося завантажити стоп-бренди Epicenter:\n" + "\n".join(errors)
    )


def _stop_brand_index(stop_brands: Iterable[str]) -> dict[str, str]:
    index: dict[str, str] = {}
    for brand in stop_brands:
        norm = _normalize_brand(brand)
        if norm:
            index.setdefault(norm, _clean_text(brand))
    return index


def _brand_candidates(body: str) -> list[str]:
    candidates: list[str] = []

    vendor_match = _VENDOR_RE.search(body)
    if vendor_match:
        candidates.append(_clean_text(vendor_match.group(1)))

    for param_match in _PROM_PARAM_RE.finditer(body):
        param_name = _normalize_brand(param_match.group(1))
        if param_name in _PROM_BRAND_PARAM_NAMES:
            candidates.append(_clean_text(param_match.group(2)))

    return [candidate for candidate in candidates if candidate]


def _matched_stop_brand(candidates: Iterable[str], stop_index: dict[str, str]) -> str | None:
    for candidate in candidates:
        for token in (candidate, *_BRAND_SPLIT_RE.split(candidate)):
            norm = _normalize_brand(token)
            if norm in stop_index:
                return stop_index[norm]
    return None


def filter_stop_brand_offers(
    xml: str,
    stop_brands: Iterable[str] | None = None,
) -> str:
    """
    Removes offers whose vendor/brand is present in the stop-brand list.

    stop_brands may be passed directly for tests; production loads Google Sheet.
    """
    raw_stop_brands = load_stop_brands() if stop_brands is None else frozenset(stop_brands)
    stop_index = _stop_brand_index(raw_stop_brands)
    if not stop_index:
        raise RuntimeError("Список стоп-брендів Epicenter порожній після нормалізації")

    removed_by_brand: Counter[str] = Counter()

    def on_offer(m: re.Match[str]) -> str:
        body = m.group(3)
        matched_brand = _matched_stop_brand(_brand_candidates(body), stop_index)
        if not matched_brand:
            return m.group(0)

        removed_by_brand[matched_brand] += 1
        return ""

    filtered_xml = _OFFER_RE.sub(on_offer, xml)

    removed_total = sum(removed_by_brand.values())
    if removed_total:
        brands_summary = ", ".join(
            f"{brand} ({count})" for brand, count in removed_by_brand.most_common()
        )
        print(f"🚫 Epicenter стоп-бренди: видалено {removed_total} товарів | {brands_summary}")
    else:
        print("🚫 Epicenter стоп-бренди: збігів у фіді немає")

    return filtered_xml
