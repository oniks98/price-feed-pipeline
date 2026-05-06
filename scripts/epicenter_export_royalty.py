"""
Script  : epicenter_export_royalty.py
Source  : https://admin.epicentrm.com.ua/public/commissions  (Angular SPA)
Output  : C:\\FullStack\\PriceFeedPipeline\\data\\markets\\royalty_epicenter.xlsx

Columns : ID категорії | Группа | Відсоток роялті | parentCode

ID source: DOM CSS class nodeId-XXXX (API interception disabled — API returns
           localisation data, not the category tree).

Install deps (once):
    pip install playwright openpyxl beautifulsoup4
    playwright install chromium

Usage:
    python epicenter_export_royalty.py             # live fetch + export
    python epicenter_export_royalty.py --dry-run   # print to console only
    python epicenter_export_royalty.py --show-api  # log intercepted API URLs
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from dataclasses import dataclass, fields
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TARGET_URL  = "https://admin.epicentrm.com.ua/public/commissions"
OUTPUT_PATH = Path(r"C:\FullStack\PriceFeedPipeline\data\markets\royalty_epicenter.xlsx")

PAGE_TIMEOUT_MS   = 30_000
EXPAND_TIMEOUT_MS = 5_000
SETTLE_MS         = 600
MAX_EXPAND_PASSES = 20

# ---------------------------------------------------------------------------
# Schema  (single source of truth — 4 columns)
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class CategoryRow:
    category_id   : str   # ID категорії  (nodeId-XXXX from DOM)
    category_name : str   # Группа
    commission_pct: str   # Відсоток роялті
    parent_code   : str   # parentCode     (category_id of immediate parent)

FIELDNAMES: list[str] = [f.name for f in fields(CategoryRow)]

HEADERS: dict[str, str] = {
    "category_id"   : "ID категорії",
    "category_name" : "Группа",
    "commission_pct": "Відсоток роялті",
    "parent_code"   : "parentCode",
}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Playwright: fetch + expand all collapsed nodes
# ---------------------------------------------------------------------------

COLLAPSED_SEL = "span.toggle-children-collapsed"
TREE_NODE_SEL = "tree-node"


def _expand_all(page) -> None:
    for pass_num in range(1, MAX_EXPAND_PASSES + 1):
        collapsed = page.query_selector_all(COLLAPSED_SEL)
        if not collapsed:
            log.info("Pass %d: no collapsed nodes left -> done", pass_num)
            break
        log.info("Pass %d: clicking %d collapsed arrows...", pass_num, len(collapsed))
        for btn in collapsed:
            try:
                btn.scroll_into_view_if_needed()
                btn.click(timeout=EXPAND_TIMEOUT_MS)
            except Exception as exc:
                log.debug("Click skipped: %s", exc)
        page.wait_for_timeout(SETTLE_MS)
    else:
        log.warning(
            "Reached MAX_EXPAND_PASSES=%d — tree may be incomplete",
            MAX_EXPAND_PASSES,
        )


def fetch_page(url: str, show_api: bool = False) -> str:
    """
    Launch headless Chromium, expand all tree nodes, return full page HTML.
    API interception is used only for --show-api diagnostics.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.error(
            "playwright not installed. Run:\n"
            "  pip install playwright\n"
            "  playwright install chromium"
        )
        sys.exit(1)

    log.info("Launching Chromium -> %s", url)
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page    = browser.new_page()

        if show_api:
            intercepted: list[str] = []

            def _on_response(response) -> None:
                if "json" in response.headers.get("content-type", ""):
                    intercepted.append(response.url)

            page.on("response", _on_response)

        page.goto(url, timeout=PAGE_TIMEOUT_MS, wait_until="networkidle")
        page.wait_for_selector(TREE_NODE_SEL, timeout=PAGE_TIMEOUT_MS)
        page.wait_for_timeout(1_000)

        if show_api:
            log.info("Intercepted API URLs (%d):", len(intercepted))
            for u in intercepted:
                log.info("  %s", u)

        _expand_all(page)
        page.wait_for_load_state("networkidle", timeout=PAGE_TIMEOUT_MS)
        page.wait_for_timeout(1_000)

        html = page.content()
        browser.close()

    log.info("HTML fetched: %d bytes", len(html))
    return html


# ---------------------------------------------------------------------------
# DOM helpers
# ---------------------------------------------------------------------------

_RE_NODE_ID = re.compile(r"nodeId-(\d+)")
_RE_PERCENT = re.compile(r"[\d.,]+")


def _node_id_from_class(tag) -> str:
    """Extract numeric ID from CSS class nodeId-XXXX."""
    for cls in (tag.get("class") or []):
        m = _RE_NODE_ID.search(cls)
        if m:
            return m.group(1)
    return ""


def _title_text(node_div) -> str:
    span = node_div.find("span", class_="title")
    return span.get_text(strip=True) if span else ""


def _commission_text(node_div) -> str:
    ctrl = node_div.find(class_="node-controls-wrapper")
    if not ctrl:
        return ""
    raw = ctrl.get_text(strip=True)
    m = _RE_PERCENT.search(raw)
    if not m:
        return ""
    return m.group(0).replace(",", ".")


def _level(node_div) -> int:
    for cls in (node_div.get("class") or []):
        m = re.search(r"tree-node-level-(\d+)", cls)
        if m:
            return int(m.group(1))
    return 0


# ---------------------------------------------------------------------------
# DOM parse -> rows
# ---------------------------------------------------------------------------

def parse_rows(html: str) -> list[CategoryRow]:
    """
    Parse the fully-expanded tree HTML into CategoryRow list.

    ID  : CSS class nodeId-XXXX on the inner node-wrapper element.
    parentCode : category_id of the closest ancestor with a lower tree level.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")

    all_divs = soup.find_all(
        lambda tag: tag.name == "div"
        and any("tree-node-level-" in c for c in (tag.get("class") or []))
    )
    log.info("DOM: found %d tree-node divs", len(all_divs))

    rows:         list[CategoryRow]       = []
    parent_stack: list[tuple[int, str]]   = []   # (level, category_id)
    no_id = no_name = 0

    for div in all_divs:
        lvl  = _level(div)
        name = _title_text(div)
        if not name:
            no_name += 1
            continue

        pct = _commission_text(div)

        # ID from node-wrapper CSS class
        wrapper = div.find(class_=lambda c: c and "node-wrapper" in c)
        cat_id  = _node_id_from_class(wrapper) if wrapper else ""
        if not cat_id:
            no_id += 1
            log.debug("No nodeId for: %r", name)

        # Maintain parent stack: pop entries with level >= current
        while parent_stack and parent_stack[-1][0] >= lvl:
            parent_stack.pop()

        parent_code = parent_stack[-1][1] if parent_stack else ""
        parent_stack.append((lvl, cat_id))

        rows.append(CategoryRow(
            category_id   = cat_id,
            category_name = name,
            commission_pct= pct,
            parent_code   = parent_code,
        ))

    log.info(
        "Parsed: %d rows | no ID: %d | no name (skipped): %d",
        len(rows), no_id, no_name,
    )
    return rows


# ---------------------------------------------------------------------------
# Export -> XLSX  (plain: no colour, no bold, no fill)
# ---------------------------------------------------------------------------

def export_xlsx(rows: list[CategoryRow], path: Path) -> None:
    try:
        import openpyxl
        from openpyxl.utils import get_column_letter
    except ImportError:
        log.error("openpyxl not installed. Run: pip install openpyxl")
        sys.exit(1)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Epicentr Royalty"

    # Plain headers — no colour, no bold, no fill
    for col, field in enumerate(FIELDNAMES, 1):
        ws.cell(row=1, column=col, value=HEADERS[field])

    # Data rows
    for r_idx, row in enumerate(rows, 2):
        for c_idx, field in enumerate(FIELDNAMES, 1):
            ws.cell(row=r_idx, column=c_idx, value=getattr(row, field))

    # Column widths
    for col, width in {1: 14, 2: 48, 3: 18, 4: 14}.items():
        ws.column_dimensions[get_column_letter(col)].width = width

    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    log.info("Saved %d rows -> %s", len(rows), path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Epicentr commissions -> XLSX")
    parser.add_argument("--dry-run",  action="store_true", help="Print to console instead of XLSX")
    parser.add_argument("--show-api", action="store_true", help="Log intercepted API URLs (diagnostic)")
    args = parser.parse_args()

    html = fetch_page(TARGET_URL, show_api=args.show_api)
    rows = parse_rows(html)

    if not rows:
        log.error("No rows parsed. Exiting.")
        sys.exit(1)

    if args.dry_run:
        print("\t".join(HEADERS[f] for f in FIELDNAMES))
        for r in rows:
            print("\t".join(str(getattr(r, f)) for f in FIELDNAMES))
        log.info("Dry-run: %d rows", len(rows))
    else:
        export_xlsx(rows, OUTPUT_PATH)


if __name__ == "__main__":
    main()
