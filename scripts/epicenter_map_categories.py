"""
epicenter_map_categories.py

Запуск:
    python scripts/epicenter_map_categories.py

Читає epicenter_mappings.xlsx, зіставляє категорії з аркуша "Маппінг"
з аркушем "Категорії Епіцентру" за збігом слів/стемів (≥80%)
та записує назад: epicenter_category_id, Назва категорії Епіцентру, parentCode.
"""

import re
from pathlib import Path

import openpyxl
from rapidfuzz import fuzz

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
ROOT        = Path(__file__).parents[1]
OUTPUT_PATH = ROOT / "data" / "markets" / "epicenter_mappings.xlsx"

SHEET_MAPPING = "Маппінг"
SHEET_EPICENTER = "Категорії Епіцентру"

# Columns in "Маппінг"
COL_PROMO = "Категорія Прому"
COL_EPI_ID = "epicenter_category_id"
COL_EPI_NAME = "Назва категорії Епіцентру"
COL_PARENT = "parentCode"

# Columns in "Категорії Епіцентру"
EPI_COL_CODE = "code"
EPI_COL_NAME_UK = "name_uk"
EPI_COL_PARENT = "parentCode"

MATCH_THRESHOLD = 80  # percent


# ──────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────

def extract_last_segment(text: str) -> str:
    """Return the part after the last '>', or the full string if none."""
    if not text:
        return ""
    parts = text.split(">")
    return parts[-1].strip()


def normalize(text: str) -> str:
    """Lowercase, remove punctuation, collapse spaces."""
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    return re.sub(r"\s+", " ", text).strip()


def stem_uk(word: str) -> str:
    """
    Lightweight Ukrainian/Russian stemmer:
    strips common suffixes so that корінь / корня / кореня all match.
    Good enough for category matching without heavy NLP dependencies.
    """
    suffixes = [
        "ання", "ення", "іння", "яння",
        "ація", "яція", "ування", "ювання",
        "ський", "зький", "цький",
        "ний", "ній", "ова", "ові", "ого",
        "ів", "ій", "ій", "их", "ах",
        "ам", "ом", "ем", "им",
        "ний", "ной", "ных", "ным",
        "ский", "зкий", "ский",
        "ание", "ение", "ование",
        "ации", "ий", "ые", "ого",
        "ный", "ной",
        "ов", "ев", "ам", "ом",
        "ий", "ая", "ое",
    ]
    w = word
    for sfx in sorted(suffixes, key=len, reverse=True):
        if w.endswith(sfx) and len(w) - len(sfx) >= 3:
            return w[: len(w) - len(sfx)]
    return w


def tokenize(text: str) -> list[str]:
    """Normalize → split → stem each token."""
    tokens = normalize(text).split()
    return [stem_uk(t) for t in tokens if len(t) > 2]


def token_overlap_score(query_tokens: list[str], target_tokens: list[str]) -> float:
    """
    Percentage of query tokens that have a fuzzy match (≥80) in target tokens.
    Returns 0–100.
    """
    if not query_tokens or not target_tokens:
        return 0.0
    matched = 0
    for qt in query_tokens:
        for tt in target_tokens:
            if fuzz.ratio(qt, tt) >= 80:
                matched += 1
                break
    return (matched / len(query_tokens)) * 100


def best_match(
    query: str,
    epi_rows: list[dict],
) -> dict | None:
    """
    Find the best matching Epicenter row for a given query string.
    Returns the row dict or None if no match reaches MATCH_THRESHOLD.
    """
    seg = extract_last_segment(query)
    q_tokens = tokenize(seg)

    best_score = 0.0
    best_row: dict | None = None

    for row in epi_rows:
        target_tokens = tokenize(row[EPI_COL_NAME_UK] or "")

        score_tokens = token_overlap_score(q_tokens, target_tokens) if q_tokens else 0
        ratio = fuzz.partial_ratio(normalize(seg), normalize(row[EPI_COL_NAME_UK] or ""))

        score = max(score_tokens, ratio)

        if score > best_score:
            best_score = score
            best_row = row

    if best_score >= MATCH_THRESHOLD:
        print(f"  ✓ [{best_score:.0f}%] '{seg}' → '{best_row[EPI_COL_NAME_UK]}'")
        return best_row

    print(f"  ✗ [{best_score:.0f}%] '{seg}' — no match")
    return None


# ──────────────────────────────────────────────
# Sheet readers
# ──────────────────────────────────────────────

def read_sheet_as_dicts(ws) -> tuple[list[dict], dict[str, int]]:
    """
    Returns (rows_as_dicts, header_col_index).
    header_col_index maps column name → 1-based column index.
    """
    headers: dict[str, int] = {}
    rows: list[dict] = []

    for i, row in enumerate(ws.iter_rows(values_only=True)):
        if i == 0:
            for j, cell in enumerate(row):
                if cell is not None:
                    headers[str(cell).strip()] = j + 1
            continue
        if all(v is None for v in row):
            continue
        record = {
            str(ws.cell(1, j + 1).value or "").strip(): (cell if cell is not None else "")
            for j, cell in enumerate(row)
        }
        rows.append(record)

    return rows, headers


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────

def main() -> None:
    if not OUTPUT_PATH.exists():
        raise FileNotFoundError(f"File not found: {OUTPUT_PATH}")

    wb = openpyxl.load_workbook(OUTPUT_PATH)

    if SHEET_MAPPING not in wb.sheetnames:
        raise ValueError(
            f"Sheet '{SHEET_MAPPING}' not found. Available: {wb.sheetnames}"
        )
    if SHEET_EPICENTER not in wb.sheetnames:
        raise ValueError(
            f"Sheet '{SHEET_EPICENTER}' not found. Available: {wb.sheetnames}"
        )

    ws_map = wb[SHEET_MAPPING]
    ws_epi = wb[SHEET_EPICENTER]

    # Read Epicenter categories into memory
    epi_rows, _ = read_sheet_as_dicts(ws_epi)
    print(f"Loaded {len(epi_rows)} Epicenter categories.")

    # Resolve header columns in Mapping sheet
    map_headers: dict[str, int] = {}
    for j, cell in enumerate(ws_map[1]):
        if cell.value is not None:
            map_headers[str(cell.value).strip()] = j + 1

    # Ensure output columns exist in header row
    def ensure_col(name: str) -> int:
        if name not in map_headers:
            next_col = max(map_headers.values()) + 1
            ws_map.cell(1, next_col, name)
            map_headers[name] = next_col
            print(f"  Added column '{name}' at position {next_col}")
        return map_headers[name]

    col_epi_id = ensure_col(COL_EPI_ID)
    col_epi_name = ensure_col(COL_EPI_NAME)
    col_parent = ensure_col(COL_PARENT)

    col_promo = map_headers.get(COL_PROMO)

    if not col_promo:
        raise ValueError(
            f"Column '{COL_PROMO}' not found. Found: {list(map_headers.keys())}"
        )

    # Iterate data rows (skip header row 1)
    updated = 0
    skipped = 0
    already_matched = 0

    for row_idx in range(2, ws_map.max_row + 1):
        val = ws_map.cell(row_idx, col_promo).value or ""

        if not str(val).strip():
            continue  # empty row

        # Skip rows where all three output columns are already filled
        already_epi_id   = ws_map.cell(row_idx, col_epi_id).value
        already_epi_name = ws_map.cell(row_idx, col_epi_name).value

        if already_epi_id and already_epi_name:
            already_matched += 1
            continue  # matching already done — skip

        print(f"Row {row_idx}: '{val}'")

        match = best_match(str(val), epi_rows)

        if match:
            ws_map.cell(row_idx, col_epi_id).value   = match.get(EPI_COL_CODE, "")
            ws_map.cell(row_idx, col_epi_name).value = match.get(EPI_COL_NAME_UK, "")
            ws_map.cell(row_idx, col_parent).value   = match.get(EPI_COL_PARENT, "")
            updated += 1
        else:
            skipped += 1

    wb.save(OUTPUT_PATH)
    print(
        f"\nDone. Updated: {updated} | Skipped (no match): {skipped} "
        f"| Already matched (skipped): {already_matched}"
    )
    print(f"Saved → {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
