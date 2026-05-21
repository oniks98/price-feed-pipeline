"""
python scripts/products_export_viatec.py

Витягує всі URL фільтрів-чекбоксів зі сторінки каталогу viatec.ua
та записує рядки (url, name, category) у CSV — кожен запис повторюється
REPEAT_COUNT разів поспіль, пропускаючи налаштовані виключені групи.

Формат URL у виводі:
    data-href:  https://viatec.ua/catalog/elektroinstrumenti/tip-instrumentu-elektro:frezer
    записується: https://viatec.ua/catalog/elektroinstrumenti/0:0;tip-instrumentu-elektro:frezer

Налаштування (редагуйте тільки блок нижче):
    CATALOG_URL     — сторінка каталогу постачальника для старту
    OUTPUT_CSV      — вихідний CSV-файл
    EXCLUDED_GROUPS — назви груп фільтрів, які потрібно пропустити
    REPEAT_COUNT    — кількість повторень кожного рядка (за замовчуванням: 2)
"""

from __future__ import annotations

import csv
import logging
import sys
import time
from pathlib import Path
from typing import Iterator

import requests
from bs4 import BeautifulSoup, Tag

# ── Configuration (edit here only) ───────────────────────────────────────────

CATALOG_URL: str = "https://viatec.ua/catalog/alarm-detector"

OUTPUT_CSV: Path = Path(
    r"C:\FullStack\PriceFeedPipeline\data\markets\products_export_viatec.csv"
)

EXCLUDED_GROUPS: frozenset[str] = frozenset({"Наявність", "Бонуси"})

REPEAT_COUNT: int = 2

# ── Constants (do not edit) ───────────────────────────────────────────────────

CSV_COLUMNS: tuple[str, ...] = ("url", "name", "category")

CSV_DELIMITER: str = ";"  # semicolon for Ukrainian/European Excel locale

# Prefix inserted between the base catalog URL and the filter slug.
# Result: {CATALOG_URL}/{FILTER_PREFIX}{slug}
# e.g.  .../elektroinstrumenti/0:0;tip-instrumentu-elektro:frezer
FILTER_PREFIX: str = "0:0;"

REQUEST_TIMEOUT: int = 30
REQUEST_HEADERS: dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "uk-UA,uk;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_catalog_page(url: str) -> BeautifulSoup:
    """Fetch the catalog page and return a parsed BeautifulSoup tree."""
    log.info("Fetching catalog: %s", url)
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    log.info("Response status: %d | size: %d bytes", response.status_code, len(response.content))
    return BeautifulSoup(response.text, "html.parser")


# ── URL formatting ────────────────────────────────────────────────────────────

def _format_filter_url(raw_href: str) -> str:
    """
    Convert a raw filter data-href into the export URL format.

    Input:  https://viatec.ua/catalog/elektroinstrumenti/tip-instrumentu-elektro:frezer
    Output: https://viatec.ua/catalog/elektroinstrumenti/0:0;tip-instrumentu-elektro:frezer

    Strategy: strip the CATALOG_URL prefix, take the remaining slug,
    and rebuild as {CATALOG_URL}/{FILTER_PREFIX}{slug}.
    Falls back to the raw href unchanged if the prefix is not found.
    """
    base = CATALOG_URL.rstrip("/")
    if not raw_href.startswith(base + "/"):
        log.warning("URL does not match CATALOG_URL base, using as-is: %s", raw_href)
        return raw_href

    slug = raw_href[len(base) + 1:]  # everything after the trailing slash
    return f"{base}/{FILTER_PREFIX}{slug}"


# ── Extract ───────────────────────────────────────────────────────────────────

def _resolve_group_name(group_el: Tag) -> str | None:
    """Return the trimmed header text for a filter group element, or None."""
    header = group_el.select_one("span.filter-main__element-header-text")
    return header.get_text(strip=True) if header else None


def _resolve_checkbox_record(
    checkbox: Tag,
    category: str,
) -> dict[str, str] | None:
    """
    Extract (url, name, category) from a single checkbox <input> element.
    URL is transformed via _format_filter_url before storing.
    Returns None and logs a warning if any critical field is missing.
    """
    raw_href: str = (checkbox.get("data-href") or "").strip()
    if not raw_href:
        log.warning("Checkbox has no data-href in category '%s' — skipped", category)
        return None

    parent_label: Tag | None = checkbox.find_parent("label")
    name_el = (
        parent_label.select_one("span.filter-main__label-text")
        if parent_label
        else None
    )
    name: str = name_el.get_text(strip=True) if name_el else ""

    if not name:
        log.warning("Empty name for url=%s in category '%s' — skipped", raw_href, category)
        return None

    return {
        "url": _format_filter_url(raw_href),
        "name": name,
        "category": category,
    }


def iter_filter_records(soup: BeautifulSoup) -> Iterator[dict[str, str]]:
    """
    Walk every filter group on the page in DOM order.
    Skips groups listed in EXCLUDED_GROUPS.
    Yields each valid checkbox record REPEAT_COUNT times consecutively.
    """
    filter_groups: list[Tag] = soup.select("li.filter-main__element")
    log.info("Total filter group elements found: %d", len(filter_groups))

    for group in filter_groups:
        category = _resolve_group_name(group)

        if category is None:
            log.debug("Filter group without header text — skipped")
            continue

        if category in EXCLUDED_GROUPS:
            log.info("Excluded group: '%s'", category)
            continue

        checkboxes: list[Tag] = group.select("input.filter-main__element-checkbox")

        if not checkboxes:
            log.debug("No checkboxes in group '%s' — skipped", category)
            continue

        log.info("Group '%s': %d checkbox(es)", category, len(checkboxes))

        for checkbox in checkboxes:
            record = _resolve_checkbox_record(checkbox, category)
            if record is None:
                continue
            for _ in range(REPEAT_COUNT):
                yield record


# ── Export ────────────────────────────────────────────────────────────────────

def export_csv(records: Iterator[dict[str, str]], output_path: Path) -> int:
    """
    Stream-write records to a UTF-8 BOM CSV file with semicolon delimiter.
    Creates parent directories if needed.
    Returns the total number of rows written.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    written = 0
    with output_path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(CSV_COLUMNS), delimiter=CSV_DELIMITER)
        writer.writeheader()
        for record in records:
            writer.writerow({col: record.get(col, "") for col in CSV_COLUMNS})
            written += 1

    return written


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    t0 = time.monotonic()

    soup = fetch_catalog_page(CATALOG_URL)
    records = iter_filter_records(soup)
    total_rows = export_csv(records, OUTPUT_CSV)

    elapsed = time.monotonic() - t0
    log.info(
        "Finished. Rows written: %d | Output: %s | Elapsed: %.2fs",
        total_rows,
        OUTPUT_CSV,
        elapsed,
    )


if __name__ == "__main__":
    main()
