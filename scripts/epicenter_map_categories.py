"""
epicenter_map_categories.py

Запуск:
    python scripts/epicenter_map_categories.py

Reads epicenter_mappings.xlsx, matches categories from sheet "Маппінг"
against sheet "Категорії Епіцентру" using word/stem overlap (≥60%),
and writes back: epicenter_category_id, Назва категорії Епіцентру, parentCode.
"""

import re
from pathlib import Path

import openpyxl
from rapidfuzz import fuzz

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
XLSX_PATH = Path(r"C:\FullStack\Scrapy\data\markets\epicenter_mappings.xlsx")

SHEET_MAPPING = "Маппінг"
SHEET_EPICENTER = "Категорії Епіцентру"

# Columns in "Маппінг"
COL_PROMO_UK = "Категорія Прому  укр"
COL_PROMO_RU = "Категорія Прому  рус"
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
    query_uk: str,
    query_ru: str,
    epi_rows: list[dict],
) -> dict | None:
    """
    Find the best matching Epicenter row for given Ukrainian + Russian query strings.
    Returns the row dict or None if no match reaches MATCH_THRESHOLD.
    """
    seg_uk = extract_last_segment(query_uk)
    seg_ru = extract_last_segment(query_ru)

    q_tokens_uk = tokenize(seg_uk)
    q_tokens_ru = tokenize(seg_ru)

    best_score = 0.0
    best_row: dict | None = None

    for row in epi_rows:
        target_tokens = tokenize(row[EPI_COL_NAME_UK] or "")

        # Score against Ukrainian query
        score_uk = token_overlap_score(q_tokens_uk, target_tokens) if q_tokens_uk else 0
        # Score against Russian query (fallback / extra signal)
        score_ru = token_overlap_score(q_tokens_ru, target_tokens) if q_tokens_ru else 0

        # Also try full fuzzy ratio on normalized strings (handles short labels well)
        ratio_uk = fuzz.partial_ratio(normalize(seg_uk), normalize(row[EPI_COL_NAME_UK] or ""))
        ratio_ru = fuzz.partial_ratio(normalize(seg_ru), normalize(row[EPI_COL_NAME_UK] or ""))

        score = max(score_uk, score_ru, ratio_uk, ratio_ru)

        if score > best_score:
            best_score = score
            best_row = row

    if best_score >= MATCH_THRESHOLD:
        print(f"  ✓ [{best_score:.0f}%] '{seg_uk}' → '{best_row[EPI_COL_NAME_UK]}'")
        return best_row

    print(f"  ✗ [{best_score:.0f}%] '{seg_uk}' — no match")
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
    if not XLSX_PATH.exists():
        raise FileNotFoundError(f"File not found: {XLSX_PATH}")

    wb = openpyxl.load_workbook(XLSX_PATH)

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

    col_uk = map_headers.get(COL_PROMO_UK)
    col_ru = map_headers.get(COL_PROMO_RU)

    if not col_uk:
        raise ValueError(
            f"Column '{COL_PROMO_UK}' not found. Found: {list(map_headers.keys())}"
        )
    if not col_ru:
        raise ValueError(
            f"Column '{COL_PROMO_RU}' not found. Found: {list(map_headers.keys())}"
        )

    # Iterate data rows (skip header row 1)
    updated = 0
    skipped = 0

    for row_idx in range(2, ws_map.max_row + 1):
        val_uk = ws_map.cell(row_idx, col_uk).value or ""
        val_ru = ws_map.cell(row_idx, col_ru).value or ""

        if not str(val_uk).strip() and not str(val_ru).strip():
            continue  # empty row

        print(f"Row {row_idx}: '{val_uk}' / '{val_ru}'")

        match = best_match(str(val_uk), str(val_ru), epi_rows)

        if match:
            ws_map.cell(row_idx, col_epi_id).value = match.get(EPI_COL_CODE, "")
            ws_map.cell(row_idx, col_epi_name).value = match.get(EPI_COL_NAME_UK, "")
            ws_map.cell(row_idx, col_parent).value = match.get(EPI_COL_PARENT, "")
            updated += 1
        else:
            skipped += 1

    wb.save(XLSX_PATH)
    print(f"\nDone. Updated: {updated} rows | Skipped (no match): {skipped} rows.")
    print(f"Saved → {XLSX_PATH}")


if __name__ == "__main__":
    main()
