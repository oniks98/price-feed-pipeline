"""
services/market_coefficients.py
--------------------------------
Єдина точка читання data/markets/markets_coefficients.csv.

CSV читається один раз (lru_cache) і роздається всім споживачам.
Доступний інтерфейс:
    get_coefficients(market)        → {category_id: Decimal}
    get_default_coefficient(market) → Decimal   (рядок category_id=0)

market: 'rozetka' | 'epicenter' | 'kasta'
"""

import csv
from decimal import Decimal
from functools import lru_cache
from pathlib import Path
from typing import Final

# services/ → scripts/ → project root → data/markets/
_CSV_PATH: Final[Path] = (
    Path(__file__).parents[2] / "data" / "markets" / "markets_coefficients.csv"
)
_DEFAULT_ROW_ID: Final[str] = "0"   # uncategorized — DEFAULT_COEFFICIENT


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _detect_encoding(path: Path) -> str:
    raw = path.read_bytes()[:10_000]
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    for enc in ("utf-8", "utf-8-sig", "windows-1251", "latin-1"):
        try:
            raw.decode(enc)
            return enc
        except (UnicodeDecodeError, LookupError):
            continue
    return "utf-8-sig"


def _detect_delimiter(path: Path, encoding: str) -> str:
    with path.open(encoding=encoding) as f:
        first = f.readline()
    return ";" if ";" in first else ","


# ---------------------------------------------------------------------------
# Cached loader — CSV читається рівно один раз на процес
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_rows() -> list[dict[str, str]]:
    if not _CSV_PATH.exists():
        raise FileNotFoundError(
            f"markets_coefficients.csv не знайдено: {_CSV_PATH}"
        )
    enc = _detect_encoding(_CSV_PATH)
    delim = _detect_delimiter(_CSV_PATH, enc)
    with _CSV_PATH.open(encoding=enc, errors="replace", newline="") as f:
        return list(csv.DictReader(f, delimiter=delim))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_coefficients(market: str) -> dict[str, Decimal]:
    """
    Повертає {category_id: Decimal} для вказаного маркетплейсу.
    Невалідні рядки пропускаються без виключення.
    """
    col = f"coef_{market}"
    result: dict[str, Decimal] = {}

    for row in _load_rows():
        cat_id = (row.get("category_id") or "").strip().strip("\ufeff")
        raw = (row.get(col) or "").strip().replace(",", ".")
        try:
            result[cat_id] = Decimal(raw)
        except Exception:
            pass

    print(f"📋 Завантажено {len(result)} категорій з коефіцієнтами (coef_{market})")
    return result


def get_default_coefficient(market: str) -> Decimal:
    """
    Читає DEFAULT_COEFFICIENT з рядка category_id=0 (uncategorized).
    Кидає ValueError якщо рядок відсутній або значення невалідне —
    тихого fallback немає: некоректний коефіцієнт → невірні ціни.
    """
    col = f"coef_{market}"

    for row in _load_rows():
        cat_id = (row.get("category_id") or "").strip().strip("\ufeff")
        if cat_id != _DEFAULT_ROW_ID:
            continue
        raw = (row.get(col) or "").strip().replace(",", ".")
        try:
            value = Decimal(raw)
            print(f"📌 DEFAULT_COEFFICIENT (coef_{market}): {value}")
            return value
        except Exception:
            raise ValueError(
                f"Невалідний DEFAULT_COEFFICIENT у CSV: {raw!r} (колонка {col!r})"
            )

    raise ValueError(
        f"Рядок category_id={_DEFAULT_ROW_ID} (uncategorized) не знайдено у {_CSV_PATH} "
        f"— неможливо визначити DEFAULT_COEFFICIENT для 'coef_{market}'"
    )
