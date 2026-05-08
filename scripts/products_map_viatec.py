"""
python scripts/products_map_viatec.py
======================
Сопоставление категорий viatec → Prom.ua с точностью >= 80%.

Логика сопоставления (по приоритету):
  Категория4 → Категория3 → Категория2 → Категория1

Для каждого уникального `name` из viatec-файла последовательно ищем
наилучшее совпадение в столбцах Prom.ua (от специфичного к общему).
Первый уровень, где score >= MATCH_THRESHOLD, побеждает.

Результат: запись обратно в products_export_viatec.csv
           (через временный файл — атомарная замена).
           Отсутствующие столбцы Адрес_подраздела, Идентификатор_подраздела,
           Категория добавляются в конец автоматически.

Зависимости: rapidfuzz
  pip install rapidfuzz
"""

from __future__ import annotations

import csv
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import TypedDict

from rapidfuzz import fuzz, process

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[1]

VIATEC_CSV = BASE_DIR / "data" / "markets" / "products_export_viatec.csv"
PROM_CSV = BASE_DIR / "data" / "markets" / "Prom.ua_categories_03_05_2026.csv"

MATCH_THRESHOLD: int = 80
CSV_DELIMITER: str = ";"
CSV_ENCODING: str = "utf-8-sig"

CATEGORY_PRIORITY: list[str] = [
    "Категория4",
    "Категория3",
    "Категория2",
    "Категория1",
]

PROM_ADDR_COL = "Адрес_подраздела"
PROM_ID_COL = "Идентификатор_подраздела"

VIATEC_NAME_COL = "name"
VIATEC_ADDR_COL = "Адрес_подраздела"
VIATEC_ID_COL = "Идентификатор_подраздела"
VIATEC_MATCHED_COL = "Категория"

# Все столбцы, которые должны присутствовать в файле viatec.
# Если какого-то нет — добавляется в конец в этом порядке.
REQUIRED_OUTPUT_COLS: list[str] = [
    VIATEC_ADDR_COL,
    VIATEC_ID_COL,
    VIATEC_MATCHED_COL,
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class PromRow(TypedDict):
    Категория1: str
    Категория2: str
    Категория3: str
    Категория4: str
    Адрес_подраздела: str
    Идентификатор_подраздела: str


class MatchResult(TypedDict):
    addr: str
    prom_id: str
    matched_category: str
    matched_value: str
    score: float


# ---------------------------------------------------------------------------
# Step 1 — Load Prom.ua index
# ---------------------------------------------------------------------------

def load_prom_index(path: Path) -> dict[str, list[tuple[str, PromRow]]]:
    """
    Строит индекс:
      { "Категория4": [(value, row), ...], "Категория3": [...], ... }

    Только непустые значения категорий попадают в индекс.
    """
    index: dict[str, list[tuple[str, PromRow]]] = {col: [] for col in CATEGORY_PRIORITY}

    with path.open(encoding=CSV_ENCODING, newline="") as fh:
        reader = csv.DictReader(fh, delimiter=CSV_DELIMITER)
        for row in reader:
            prom_row = PromRow(
                Категория1=row.get("Категория1", "").strip(),
                Категория2=row.get("Категория2", "").strip(),
                Категория3=row.get("Категория3", "").strip(),
                Категория4=row.get("Категория4", "").strip(),
                Адрес_подраздела=row.get(PROM_ADDR_COL, "").strip(),
                Идентификатор_подраздела=row.get(PROM_ID_COL, "").strip(),
            )
            for col in CATEGORY_PRIORITY:
                val = prom_row[col]  # type: ignore[literal-required]
                if val:
                    index[col].append((val, prom_row))

    for col, entries in index.items():
        log.info("Prom index | %-12s → %d значений", col, len(entries))

    return index


# ---------------------------------------------------------------------------
# Step 2 — Fuzzy match per unique name
# ---------------------------------------------------------------------------

def best_match(
    query: str,
    index: dict[str, list[tuple[str, PromRow]]],
) -> MatchResult | None:
    """
    Перебирает уровни CATEGORY_PRIORITY по порядку.
    Возвращает первый результат с score >= MATCH_THRESHOLD.
    """
    for col in CATEGORY_PRIORITY:
        entries = index[col]
        if not entries:
            continue

        choices = [v for v, _ in entries]
        result = process.extractOne(
            query,
            choices,
            scorer=fuzz.WRatio,
            score_cutoff=MATCH_THRESHOLD,
        )
        if result is None:
            continue

        matched_value, score, idx = result
        _, prom_row = entries[idx]

        return MatchResult(
            addr=prom_row[PROM_ADDR_COL],  # type: ignore[literal-required]
            prom_id=prom_row[PROM_ID_COL],  # type: ignore[literal-required]
            matched_category=col,
            matched_value=matched_value,
            score=score,
        )

    return None


def build_name_cache(
    viatec_path: Path,
    index: dict[str, list[tuple[str, PromRow]]],
) -> dict[str, MatchResult | None]:
    """
    Собирает уникальные name из viatec, выполняет fuzzy-matching,
    возвращает кэш: { name: MatchResult | None }.
    """
    unique_names: set[str] = set()
    with viatec_path.open(encoding=CSV_ENCODING, newline="") as fh:
        reader = csv.DictReader(fh, delimiter=CSV_DELIMITER)
        for row in reader:
            name = row.get(VIATEC_NAME_COL, "").strip()
            if name:
                unique_names.add(name)

    log.info("Уникальных name в viatec: %d", len(unique_names))

    cache: dict[str, MatchResult | None] = {}
    matched = missed = 0

    for name in sorted(unique_names):
        result = best_match(name, index)
        cache[name] = result
        if result:
            matched += 1
            log.debug(
                "MATCH | %-40s → [%s] %-40s (%.0f%%)",
                name,
                result["matched_category"],
                result["matched_value"],
                result["score"],
            )
        else:
            missed += 1
            log.warning("NO MATCH | %s", name)

    log.info(
        "Результат: %d совпало / %d не найдено (порог %d%%)",
        matched,
        missed,
        MATCH_THRESHOLD,
    )
    return cache


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def ensure_columns(fieldnames: list[str], required: list[str]) -> list[str]:
    """
    Возвращает fieldnames с добавленными в конец отсутствующими столбцами.
    Порядок добавления соответствует порядку в `required`.
    """
    result = list(fieldnames)
    for col in required:
        if col not in result:
            result.append(col)
            log.info("Столбец отсутствовал в файле, добавлен: %s", col)
    return result


# ---------------------------------------------------------------------------
# Step 3 — Write back to the same file (atomic via temp file)
# ---------------------------------------------------------------------------

def write_inplace(
    viatec_path: Path,
    cache: dict[str, MatchResult | None],
) -> None:
    """
    Читает viatec CSV, подставляет совпадения из кэша,
    записывает во временный файл в той же директории,
    затем атомарно заменяет оригинал (os.replace).

    - Отсутствующие столбцы (Адрес_подраздела, Идентификатор_подраздела,
      Категория) добавляются в конец автоматически.
    - Строки с уже заполненными всеми тремя полями не перезаписываются
      (идемпотентность).
    """
    tmp_path: Path | None = None

    try:
        fd, tmp_str = tempfile.mkstemp(
            dir=viatec_path.parent,
            prefix=".tmp_viatec_",
            suffix=".csv",
        )
        tmp_path = Path(tmp_str)

        with (
            viatec_path.open(encoding=CSV_ENCODING, newline="") as in_fh,
            os.fdopen(fd, "w", encoding=CSV_ENCODING, newline="") as out_fh,
        ):
            reader = csv.DictReader(in_fh, delimiter=CSV_DELIMITER)
            assert reader.fieldnames, "Не удалось прочитать заголовок viatec CSV"

            # Гарантируем наличие всех нужных столбцов
            fieldnames = ensure_columns(list(reader.fieldnames), REQUIRED_OUTPUT_COLS)

            writer = csv.DictWriter(
                out_fh,
                fieldnames=fieldnames,
                delimiter=CSV_DELIMITER,
                quoting=csv.QUOTE_MINIMAL,
                extrasaction="ignore",
            )
            writer.writeheader()

            written = skipped = 0

            for row in reader:
                name = row.get(VIATEC_NAME_COL, "").strip()

                # Уже полностью заполнено — не трогаем (идемпотентность)
                if (
                    row.get(VIATEC_ADDR_COL, "").strip()
                    and row.get(VIATEC_ID_COL, "").strip()
                    and row.get(VIATEC_MATCHED_COL, "").strip()
                ):
                    writer.writerow(row)
                    written += 1
                    continue

                result = cache.get(name)
                if result:
                    row[VIATEC_ADDR_COL] = result["addr"]
                    row[VIATEC_ID_COL] = result["prom_id"]
                    row[VIATEC_MATCHED_COL] = result["matched_value"]
                    written += 1
                else:
                    # Гарантируем наличие ключей для новых столбцов
                    for col in REQUIRED_OUTPUT_COLS:
                        row.setdefault(col, "")
                    skipped += 1

                writer.writerow(row)

        os.replace(tmp_path, viatec_path)
        tmp_path = None

        log.info("Записано строк: %d | Без совпадения: %d", written, skipped)
        log.info("Файл обновлён → %s", viatec_path)

    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()
            log.debug("Временный файл удалён: %s", tmp_path)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    for path, label in [(VIATEC_CSV, "viatec"), (PROM_CSV, "Prom.ua")]:
        if not path.exists():
            log.error("Файл не найден: %s (%s)", path, label)
            sys.exit(1)

    log.info("Загружаем индекс Prom.ua категорий...")
    index = load_prom_index(PROM_CSV)

    log.info("Строим кэш совпадений для viatec...")
    cache = build_name_cache(VIATEC_CSV, index)

    log.info("Записываем результат обратно в исходный файл...")
    write_inplace(VIATEC_CSV, cache)

    log.info("Готово.")


if __name__ == "__main__":
    main()
