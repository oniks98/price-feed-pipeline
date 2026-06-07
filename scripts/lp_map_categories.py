"""
lp_map_categories.py
═══════════════════════════════════════════════════════════════════════
Автоматичний маппінг категорій LP → підрозділи Prom.ua.

Алгоритм пошуку (по пріоритету для кожного рядка):
  1. Беремо конечну (leaf) категорію з "Назва у постачальника"
     (остання частина шляху після " > ").

  2. EXACT match — шукаємо leaf точно у стовпцях Prom:
       Категория4 → Категория3 → Категория2 → Категория1

  3. WORD-OVERLAP match — витягуємо значущі слова (≥3 символів) з leaf
     і порівнюємо з кожним значенням того самого стовпця Prom.
     Збіг = усі слова коротшого набору входять до довшого.
       Приклади, що проходять:
         "Культиватори"           ↔ "Ручні культиватори, плуги"
         "Мотокоси та тримери садові" ↔ "Мотокоси і тримери"
       Пошук: Категория4 → Категория3 → Категория2 → Категория1
       У кожному стовпці листові записи (де стовпець є найглибшим) — першими.

  4. FULL-PATH fallback — точний збіг нормалізованого 4-tuple шляху.

Консервативний режим (за замовчуванням):
    Вже заповнені Посилання_підрозділу / Ідентифікатор_підрозділу — не перезаписуються.
    Прапор --overwrite примусово перезаписує існуючі значення.

Запуск:
    python scripts/lp_map_categories.py
    python scripts/lp_map_categories.py --overwrite
    python scripts/lp_map_categories.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────

ROOT = Path(__file__).parents[1]

LP_CATEGORY_CSV  = ROOT / "data" / "lp" / "lp_category.csv"
MARKETS_DIR      = ROOT / "data" / "markets"
PROM_CSV_PATTERN = "Prom.ua_categories_*.csv"

CSV_DELIMITER  = ";"
CSV_ENCODING   = "utf-8-sig"
PATH_SEPARATOR = " > "

# lp_category.csv column names
COL_SUPPLIER_NAME = "Назва у постачальника"
COL_LP_ID         = "Ідентифікатор_підрозділу"
COL_LP_URL        = "Посилання_підрозділу"
COL_CHANNEL       = "channel"

# Prom categories CSV column names
PROM_COLS    = ("Категория1", "Категория2", "Категория3", "Категория4")
PROM_URL_COL = "Адрес_подраздела"
PROM_ID_COL  = "Идентификатор_подраздела"

# Search priority: col index 3 (Категория4) → 2 → 1 → 0
_SEARCH_ORDER: tuple[int, ...] = (3, 2, 1, 0)
_LEVEL_NAMES: dict[int, str] = {3: "col4", 2: "col3", 1: "col2", 0: "col1"}
_FALLBACK_LEVEL = "full_path"

# Words shorter than this are ignored in word-overlap matching
_MIN_WORD_LEN = 3

# ─────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────
# Text helpers
# ─────────────────────────────────────────────────────────────────────

def normalize(text: str) -> str:
    """Lowercase, strip, collapse internal whitespace."""
    return re.sub(r"\s+", " ", text.strip().lower())


def split_supplier_path(supplier_name: str) -> list[str]:
    """Split supplier path by PATH_SEPARATOR into normalized parts (no empties)."""
    return [normalize(p) for p in supplier_name.split(PATH_SEPARATOR) if p.strip()]


def to_full_key(parts: list[str]) -> tuple[str, str, str, str]:
    """Pad/trim parts to a normalized 4-tuple for full-path lookup."""
    padded = (parts + ["", "", "", ""])[:4]
    return (padded[0], padded[1], padded[2], padded[3])


def meaningful_words(text: str) -> frozenset[str]:
    """
    Extract meaningful words from a normalized string.

    Keeps only Cyrillic/Latin tokens of at least _MIN_WORD_LEN characters.
    Short tokens (union conjunctions "та", "і", "й", particles "в", "з", ...)
    are naturally excluded by the length filter without a stopword list.

    Examples:
        "ручні культиватори, плуги" → {"ручні", "культиватори", "плуги"}
        "мотокоси і тримери"        → {"мотокоси", "тримери"}
        "мотокоси та тримери садові"→ {"мотокоси", "тримери", "садові"}
    """
    return frozenset(
        w for w in re.findall(r"[а-яіїєґa-zА-ЯІЇЄҐA-Z]+", text.lower())
        if len(w) >= _MIN_WORD_LEN
    )


def words_overlap_match(leaf_words: frozenset[str], prom_words: frozenset[str]) -> bool:
    """
    True if all words of the SHORTER set appear in the LONGER set.

    This handles:
      - Single leaf word contained in a multi-word Prom value:
          {"культиватори"} vs {"ручні", "культиватори", "плуги"} → ✓
      - Multi-word leaf where key words match a shorter Prom value:
          {"мотокоси", "тримери", "садові"} vs {"мотокоси", "тримери"} → ✓
      - Single word on both sides (identical, already caught by exact match):
          {"сокири"} vs {"сокири"} → ✓
    """
    if not leaf_words or not prom_words:
        return False
    overlap = leaf_words & prom_words
    return len(overlap) >= min(len(leaf_words), len(prom_words))


# ─────────────────────────────────────────────────────────────────────
# Prom categories file discovery
# ─────────────────────────────────────────────────────────────────────

def _parse_prom_file_date(path: Path) -> tuple[int, int, int]:
    """
    Parse DD_MM_YYYY from stem like "Prom.ua_categories_03_05_2026".
    Returns (year, month, day) for correct chronological comparison.
    Returns (0, 0, 0) when pattern not found.
    """
    m = re.search(r"(\d{2})_(\d{2})_(\d{4})", path.stem)
    if not m:
        return (0, 0, 0)
    day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    return (year, month, day)


def find_latest_prom_csv(markets_dir: Path) -> Path:
    """Return the most recent Prom.ua_categories_*.csv by date in filename."""
    candidates = list(markets_dir.glob(PROM_CSV_PATTERN))
    if not candidates:
        raise FileNotFoundError(
            f"No file matching '{PROM_CSV_PATTERN}' found in {markets_dir}"
        )
    latest = max(candidates, key=_parse_prom_file_date)
    log.info("Prom categories file: %s", latest.name)
    return latest


# ─────────────────────────────────────────────────────────────────────
# Prom lookup
# ─────────────────────────────────────────────────────────────────────

# Type alias for a single Prom entry in word-overlap list
# (normalized_col_value, precomputed_word_set, (url, prom_id))
_WordEntry = tuple[str, frozenset[str], tuple[str, str]]


@dataclass
class PromLookup:
    """
    Three-tier lookup structure.

    by_col[i]       — exact lookup: normalized Категория(i+1) value → (url, prom_id).
    by_col_words[i] — word-overlap list: (_WordEntry tuples), leaf entries first.
    full_path       — exact 4-tuple path → (url, prom_id). Fallback only.

    Indexes: 0 = Категория1, 1 = Категория2, 2 = Категория3, 3 = Категория4.
    """
    by_col: list[dict[str, tuple[str, str]]] = field(
        default_factory=lambda: [{}, {}, {}, {}]
    )
    by_col_words: list[list[_WordEntry]] = field(
        default_factory=lambda: [[], [], [], []]
    )
    full_path: dict[tuple[str, str, str, str], tuple[str, str]] = field(
        default_factory=dict
    )


def _leaf_level(cols: list[str]) -> int:
    """Return the index of the last non-empty column value (-1 if all empty)."""
    for i in range(len(cols) - 1, -1, -1):
        if cols[i]:
            return i
    return -1


def _read_prom_rows(
    prom_csv: Path,
) -> list[tuple[tuple[str, str, str, str], str, str, list[str]]]:
    """
    Stream Prom CSV into a list of:
        (full_key_4tuple, url, prom_id, [norm_col1, norm_col2, norm_col3, norm_col4])

    Rows without url and prom_id are skipped.
    """
    result: list[tuple[tuple[str, str, str, str], str, str, list[str]]] = []

    with open(prom_csv, encoding=CSV_ENCODING, newline="") as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        for row in reader:
            url     = (row.get(PROM_URL_COL, "") or "").strip()
            prom_id = (row.get(PROM_ID_COL,  "") or "").strip()
            if not url and not prom_id:
                continue

            cols: list[str] = [normalize(row.get(col, "") or "") for col in PROM_COLS]
            full_key: tuple[str, str, str, str] = (cols[0], cols[1], cols[2], cols[3])
            result.append((full_key, url, prom_id, cols))

    return result


def build_prom_lookup(prom_csv: Path) -> PromLookup:
    """
    Build PromLookup from Prom categories CSV.

    Two-pass construction for leaf-entry priority in both by_col and by_col_words:
      Pass 1 — only rows where that column IS the deepest (leaf) level.
      Pass 2 — fill remaining gaps with intermediate (non-leaf) entries.

    This ensures e.g. "Сокири" stored in col3 as a true leaf wins over "Сокири"
    appearing as an intermediate node in some deeper path.
    """
    prom_rows = _read_prom_rows(prom_csv)
    lookup = PromLookup()
    duplicates_full = 0

    # ── full_path: first occurrence wins ────────────────────────────
    for full_key, url, prom_id, _ in prom_rows:
        if full_key not in lookup.full_path:
            lookup.full_path[full_key] = (url, prom_id)
        else:
            duplicates_full += 1

    # ── pass 1: leaf entries ─────────────────────────────────────────
    for _, url, prom_id, cols in prom_rows:
        leaf = _leaf_level(cols)
        if leaf < 0:
            continue
        col_val = cols[leaf]
        entry   = (url, prom_id)

        if col_val not in lookup.by_col[leaf]:
            lookup.by_col[leaf][col_val] = entry

        lookup.by_col_words[leaf].append(
            (col_val, meaningful_words(col_val), entry)
        )

    # ── pass 2: non-leaf entries fill remaining gaps ─────────────────
    for _, url, prom_id, cols in prom_rows:
        leaf = _leaf_level(cols)
        for col_idx, col_val in enumerate(cols):
            if not col_val or col_idx == leaf:
                continue
            entry = (url, prom_id)

            if col_val not in lookup.by_col[col_idx]:
                lookup.by_col[col_idx][col_val] = entry

            lookup.by_col_words[col_idx].append(
                (col_val, meaningful_words(col_val), entry)
            )

    log.info(
        "Prom lookup built: %d unique full-paths (%d duplicates skipped) | "
        "by_col sizes: col1=%d col2=%d col3=%d col4=%d",
        len(lookup.full_path),
        duplicates_full,
        len(lookup.by_col[0]),
        len(lookup.by_col[1]),
        len(lookup.by_col[2]),
        len(lookup.by_col[3]),
    )
    return lookup


# ─────────────────────────────────────────────────────────────────────
# Match
# ─────────────────────────────────────────────────────────────────────

def find_prom_match(
    supplier_name: str,
    lookup: PromLookup,
) -> tuple[tuple[str, str], str] | None:
    """
    Three-step search, returns ((url, prom_id), level_name) or None.

    Step 1 — EXACT: leaf == prom_col_value (col4 → col3 → col2 → col1)
    Step 2 — WORD-OVERLAP: all words of shorter set ⊆ longer set (col4 → … → col1)
    Step 3 — FULL-PATH: exact normalized 4-tuple match (fallback)

    level_name values: "col4" | "col3" | "col2" | "col1"
                     | "col4_word" | "col3_word" | "col2_word" | "col1_word"
                     | "full_path"
    """
    parts = split_supplier_path(supplier_name)
    if not parts:
        return None

    leaf = parts[-1]

    # ── Step 1: exact match ──────────────────────────────────────────
    for col_idx in _SEARCH_ORDER:
        match = lookup.by_col[col_idx].get(leaf)
        if match:
            return match, _LEVEL_NAMES[col_idx]

    # ── Step 2: word-overlap match ───────────────────────────────────
    leaf_words = meaningful_words(leaf)
    if leaf_words:
        for col_idx in _SEARCH_ORDER:
            for _prom_val, prom_words, entry in lookup.by_col_words[col_idx]:
                if words_overlap_match(leaf_words, prom_words):
                    return entry, f"{_LEVEL_NAMES[col_idx]}_word"

    # ── Step 3: full-path fallback ───────────────────────────────────
    full_key = to_full_key(parts)
    match = lookup.full_path.get(full_key)
    if match:
        return match, _FALLBACK_LEVEL

    return None


# ─────────────────────────────────────────────────────────────────────
# CSV I/O
# ─────────────────────────────────────────────────────────────────────

def load_lp_csv(path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Load lp_category.csv. Returns (header, rows)."""
    if not path.exists():
        raise FileNotFoundError(f"LP category file not found: {path}")

    with open(path, encoding=CSV_ENCODING, newline="") as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        if reader.fieldnames is None:
            raise ValueError(f"Empty or header-less CSV: {path}")
        header = list(reader.fieldnames)
        rows   = [dict(row) for row in reader]

    log.info("Loaded lp_category.csv: %d data rows.", len(rows))
    return header, rows


def save_lp_csv(path: Path, header: list[str], rows: list[dict[str, str]]) -> None:
    """Write rows back preserving original column order and BOM encoding."""
    with open(path, "w", encoding=CSV_ENCODING, newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=header,
            delimiter=CSV_DELIMITER,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


# ─────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────

def validate_lp_columns(header: list[str]) -> None:
    required = {COL_SUPPLIER_NAME, COL_LP_ID, COL_LP_URL, COL_CHANNEL}
    missing  = required - set(header)
    if missing:
        raise ValueError(
            f"Required columns missing in lp_category.csv: {sorted(missing)}\n"
            f"Found columns: {header}"
        )


# ─────────────────────────────────────────────────────────────────────
# Mapping
# ─────────────────────────────────────────────────────────────────────

def apply_mapping(
    rows: list[dict[str, str]],
    lookup: PromLookup,
    *,
    overwrite: bool,
) -> dict[str, int]:
    """
    Modify rows in-place using find_prom_match.

    Stats keys:
      matched_col{1-4}       — exact match at that column level
      matched_col{1-4}_word  — word-overlap match at that column level
      matched_full_path      — full-path fallback match
      skipped                — already filled, overwrite=False
      no_match               — find_prom_match returned None
      empty_name             — COL_SUPPLIER_NAME is blank
    """
    stats: dict[str, int] = {
        "matched_col4":      0,
        "matched_col3":      0,
        "matched_col2":      0,
        "matched_col1":      0,
        "matched_col4_word": 0,
        "matched_col3_word": 0,
        "matched_col2_word": 0,
        "matched_col1_word": 0,
        "matched_full_path": 0,
        "skipped":           0,
        "no_match":          0,
        "empty_name":        0,
    }

    for row in rows:
        supplier_name = (row.get(COL_SUPPLIER_NAME) or "").strip()
        channel       = (row.get(COL_CHANNEL)       or "").strip()

        if not supplier_name:
            stats["empty_name"] += 1
            continue

        existing_id  = (row.get(COL_LP_ID)  or "").strip()
        existing_url = (row.get(COL_LP_URL) or "").strip()

        if (existing_id or existing_url) and not overwrite:
            stats["skipped"] += 1
            log.debug("Skipped (already filled) [%s]: %r", channel, supplier_name)
            continue

        result = find_prom_match(supplier_name, lookup)

        if result is None:
            stats["no_match"] += 1
            log.debug("No Prom match [%s]: %r", channel, supplier_name)
            continue

        (url, prom_id), level = result
        row[COL_LP_URL] = url
        row[COL_LP_ID]  = prom_id

        stat_key = f"matched_{level}"
        stats[stat_key] = stats.get(stat_key, 0) + 1

        log.info("✓ [%s][%s] %r → id=%s", level, channel, supplier_name, prom_id)

    return stats


# ─────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────

def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Map LP supplier categories → Prom.ua "
            "Посилання_підрозділу / Ідентифікатор_підрозділу.\n\n"
            "Search: exact col4→1, then word-overlap col4→1, then full-path fallback."
        )
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite rows that already have Посилання_підрозділу / Ідентифікатор_підрозділу.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run matching but do not save changes to lp_category.csv.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])

    # ── 1. Build Prom lookup ─────────────────────────────────────────
    prom_csv = find_latest_prom_csv(MARKETS_DIR)
    lookup   = build_prom_lookup(prom_csv)

    # ── 2. Load lp_category.csv ──────────────────────────────────────
    header, rows = load_lp_csv(LP_CATEGORY_CSV)
    validate_lp_columns(header)

    # ── 3. Apply mapping ─────────────────────────────────────────────
    stats = apply_mapping(rows, lookup, overwrite=args.overwrite)

    # ── 4. Report ────────────────────────────────────────────────────
    exact_total = sum(
        stats[f"matched_col{i}"] for i in (1, 2, 3, 4)
    )
    word_total = sum(
        stats[f"matched_col{i}_word"] for i in (1, 2, 3, 4)
    )
    total_matched = exact_total + word_total + stats["matched_full_path"]

    log.info(
        "Result → matched: %d "
        "(exact: col4=%d col3=%d col2=%d col1=%d | "
        "word: col4=%d col3=%d col2=%d col1=%d | "
        "fallback=%d) "
        "| skipped: %d | no_match: %d | empty_name: %d",
        total_matched,
        stats["matched_col4"],      stats["matched_col3"],
        stats["matched_col2"],      stats["matched_col1"],
        stats["matched_col4_word"], stats["matched_col3_word"],
        stats["matched_col2_word"], stats["matched_col1_word"],
        stats["matched_full_path"],
        stats["skipped"],
        stats["no_match"],
        stats["empty_name"],
    )

    # ── 5. Save ──────────────────────────────────────────────────────
    if args.dry_run:
        log.info("Dry-run mode — lp_category.csv not saved.")
        return

    save_lp_csv(LP_CATEGORY_CSV, header, rows)
    log.info("Saved → %s", LP_CATEGORY_CSV)


if __name__ == "__main__":
    main()
