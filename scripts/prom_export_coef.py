"""
prom_export_coef.py
───────────────────
Заполняет столбик coef_prom в markets_coefficients.csv
на основе данных из Google Sheets (gid=688167001).

Таблица открыта по ссылке без авторизации — используем CSV export.

Источник (Google Sheets):
  Столбик G  → ID категорії   (category_id)
  Столбик I  → Комісія для режиму «Економ»  (X, %)

Алгоритм для каждой строки CSV:
  1. Если category_id в HARDCODED_CATEGORY_IDS → не трогаем, оставляем как есть
  2. По category_id → берём X из Google Sheets
  3. Если category_id не найден → берём coef_prom из строки FALLBACK_CATEGORY_ID (=1)
     уже проставленный в самом CSV (не из Google Sheets)
  4. Y = round(110 / (100 - (8.5 + X)), 2)
  5. Записываем Y в coef_prom нужной строки CSV

Запуск:
    python scripts/prom_export_coef.py
"""

from __future__ import annotations

import csv
import io
import logging
import sys
from pathlib import Path

import requests

# ─────────────────────────────── config ───────────────────────────────────────

BASE_DIR = Path(r"C:\FullStack\PriceFeedPipeline\data\markets")
CSV_PATH = BASE_DIR / "markets_coefficients.csv"

SPREADSHEET_ID = "1mQ86nxmPTsEj23MAAu4bGn4iKtaX-yEIiKefu4SqvkA"
SHEET_GID      = "688167001"
SHEETS_EXPORT_URL = (
    f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
    f"/export?format=csv&gid={SHEET_GID}"
)

# Индексы столбцов в Google Sheets (0-based): A=0 … G=6 … I=8
SHEETS_COL_CATEGORY_ID = 6   # G — ID категорії
SHEETS_COL_COMMISSION  = 8   # I — Комісія для режиму «Економ»

# Строка, с которой начинаются данные (1-based, строка 1 — заголовок)
SHEETS_DATA_START_ROW = 2

CSV_COL_CAT_ID    = "category_id"
CSV_COL_COEF_PROM = "coef_prom"

# category_id, чей coef_prom из CSV используется как fallback
# для категорий, не найденных в Google Sheets
FALLBACK_CATEGORY_ID = 1   # new_default_categories

# Строки с хардкод-значениями coef_prom — скрипт их не трогает
HARDCODED_CATEGORY_IDS: frozenset[int] = frozenset({
    0,   # uncategorized  (1.1)
    1,   # new_default_categories  (1.3)
})

PROM_FEE_PERCENT  = 8.5    # фиксированная комиссия Prom, %
FORMULA_NUMERATOR = 110.0  # числитель формулы

CSV_DELIMITER   = ";"
CSV_ENCODING    = "utf-8-sig"  # обрабатывает BOM автоматически
REQUEST_TIMEOUT = 30           # секунд

# ─────────────────────────────── logging ──────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────── helpers ──────────────────────────────────────

def _parse_commission(raw: object) -> float | None:
    """
    Парсит значение комиссии из ячейки:
      "7.09%"  → 7.09
      "7,09%"  → 7.09
      "7.09"   → 7.09
      ""       → None
    """
    if raw is None:
        return None
    cleaned = str(raw).strip().rstrip("%").replace(",", ".").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


# ─────────────────────────────── loader ───────────────────────────────────────

def load_prom_commissions(url: str) -> dict[int, float]:
    """
    Загружает {category_id: commission_percent} из Google Sheets CSV export.
    Таблица должна быть открыта по ссылке (без авторизации).
    """
    log.info("Загружаем данные из Google Sheets: %s", url)

    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Не удалось загрузить Google Sheets: {exc}\n"
            "Убедитесь, что таблица открыта по ссылке (Настройки доступа → "
            "«Все, у кого есть ссылка»)."
        ) from exc

    content = response.content.decode("utf-8-sig")
    reader  = csv.reader(io.StringIO(content))

    result:  dict[int, float] = {}
    skipped = 0

    for row_idx, row in enumerate(reader):
        if row_idx < SHEETS_DATA_START_ROW - 1:
            continue

        if len(row) <= max(SHEETS_COL_CATEGORY_ID, SHEETS_COL_COMMISSION):
            skipped += 1
            continue

        raw_id  = row[SHEETS_COL_CATEGORY_ID].strip()
        raw_com = row[SHEETS_COL_COMMISSION]

        if not raw_id:
            skipped += 1
            continue

        try:
            cat_id = int(raw_id)
        except (ValueError, TypeError):
            log.debug(
                "sheets row %d: невалидный ID '%s' — пропускаем",
                row_idx + 1, raw_id,
            )
            skipped += 1
            continue

        commission = _parse_commission(raw_com)
        if commission is None:
            log.warning(
                "sheets row %d: category_id=%d — невалидная комиссия '%s' — пропускаем",
                row_idx + 1, cat_id, raw_com,
            )
            skipped += 1
            continue

        result[cat_id] = commission

    log.info(
        "Google Sheets: загружено %d записей, пропущено %d строк",
        len(result), skipped,
    )
    return result


def read_fallback_coef(csv_path: Path) -> str | None:
    """
    Читает текущее значение coef_prom для FALLBACK_CATEGORY_ID из CSV.
    Возвращает строку (например '1.3') или None если не найдено / пусто.
    """
    raw    = csv_path.read_text(encoding=CSV_ENCODING)
    reader = csv.DictReader(io.StringIO(raw), delimiter=CSV_DELIMITER)

    if reader.fieldnames is None or CSV_COL_COEF_PROM not in reader.fieldnames:
        return None

    for row in reader:
        try:
            if int(row.get(CSV_COL_CAT_ID, "").strip()) == FALLBACK_CATEGORY_ID:
                coef = row.get(CSV_COL_COEF_PROM, "").strip()
                return coef if coef else None
        except (ValueError, TypeError):
            continue

    return None


# ─────────────────────────────── formula ──────────────────────────────────────

def calc_coef(commission: float) -> float:
    """Y = round(110 / (100 - (8.5 + X)), 2)"""
    denominator = 100.0 - (PROM_FEE_PERCENT + commission)
    if denominator <= 0:
        raise ValueError(
            f"Знаменатель ≤ 0 при комиссии={commission}: "
            f"100 - ({PROM_FEE_PERCENT} + {commission}) = {denominator}"
        )
    return round(FORMULA_NUMERATOR / denominator, 2)


# ─────────────────────────────── CSV processing ───────────────────────────────

def _ensure_coef_prom_column(fieldnames: list[str]) -> list[str]:
    """Добавляет coef_prom в fieldnames если отсутствует."""
    if CSV_COL_COEF_PROM not in fieldnames:
        fieldnames = list(fieldnames) + [CSV_COL_COEF_PROM]
        log.info("Столбик '%s' добавлен в CSV", CSV_COL_COEF_PROM)
    return fieldnames


def process_csv(
    csv_path: Path,
    commissions: dict[int, float],
    fallback_coef: str | None,
) -> tuple[int, int, int]:
    """
    Читает CSV, обновляет coef_prom в памяти, перезаписывает файл.

    Правила:
      - HARDCODED_CATEGORY_IDS → не трогаем
      - найден в Sheets         → считаем по формуле
      - не найден в Sheets      → fallback_coef из category_id=1

    Возвращает (updated, fallback_used, skipped_invalid_id).
    """
    raw    = csv_path.read_text(encoding=CSV_ENCODING)
    reader = csv.DictReader(io.StringIO(raw), delimiter=CSV_DELIMITER)

    if reader.fieldnames is None:
        raise RuntimeError(f"CSV {csv_path} пуст или не читается")

    if CSV_COL_CAT_ID not in reader.fieldnames:
        raise RuntimeError(f"Столбик '{CSV_COL_CAT_ID}' не найден в {csv_path}")

    fieldnames = _ensure_coef_prom_column(list(reader.fieldnames))
    rows       = list(reader)

    updated            = 0
    fallback_used      = 0
    skipped_invalid_id = 0

    for row in rows:
        raw_id = row.get(CSV_COL_CAT_ID, "").strip()

        try:
            cat_id = int(raw_id)
        except (ValueError, TypeError):
            log.warning("CSV: невалидный category_id '%s' — пропускаем", raw_id)
            skipped_invalid_id += 1
            continue

        # Хардкод-строки не трогаем
        if cat_id in HARDCODED_CATEGORY_IDS:
            log.info("category_id=%-12d  hardcoded — пропускаем", cat_id)
            continue

        commission = commissions.get(cat_id)

        if commission is not None:
            # Обычный путь: считаем коэф по формуле
            try:
                coef_str = str(calc_coef(commission))
            except ValueError as exc:
                log.error("category_id=%d: %s — пропускаем", cat_id, exc)
                continue
            log.info(
                "category_id=%-12d  commission=%-6.2f%%  coef_prom=%s",
                cat_id, commission, coef_str,
            )
        else:
            # Fallback: берём готовое значение из строки category_id=1
            if fallback_coef is None:
                log.warning(
                    "category_id=%d: не найден в Prom таблице, "
                    "fallback (category_id=%d) тоже не задан — пропускаем",
                    cat_id, FALLBACK_CATEGORY_ID,
                )
                continue
            coef_str = fallback_coef
            log.warning(
                "category_id=%d: не найден в Prom таблице → "
                "fallback coef_prom=%s (из category_id=%d)",
                cat_id, coef_str, FALLBACK_CATEGORY_ID,
            )
            fallback_used += 1

        row[CSV_COL_COEF_PROM] = coef_str
        updated += 1

    # --- перезаписываем файл ---
    out = io.StringIO()
    writer = csv.DictWriter(
        out,
        fieldnames=fieldnames,
        delimiter=CSV_DELIMITER,
        lineterminator="\n",
        extrasaction="ignore",
    )
    writer.writeheader()
    writer.writerows(rows)

    csv_path.write_text(out.getvalue(), encoding=CSV_ENCODING)
    return updated, fallback_used, skipped_invalid_id


# ─────────────────────────────── main ─────────────────────────────────────────

def main() -> None:
    log.info("=== fill_coef_prom старт ===")

    if not CSV_PATH.exists():
        log.error("Файл не найден: %s", CSV_PATH)
        sys.exit(1)

    try:
        commissions = load_prom_commissions(SHEETS_EXPORT_URL)
    except RuntimeError as exc:
        log.error("%s", exc)
        sys.exit(1)

    if not commissions:
        log.error("Prom таблица пуста или не содержит валидных данных — прерываем")
        sys.exit(1)

    # Читаем fallback из CSV до обработки
    fallback_coef = read_fallback_coef(CSV_PATH)
    if fallback_coef:
        log.info(
            "Fallback: category_id=%d → coef_prom=%s",
            FALLBACK_CATEGORY_ID, fallback_coef,
        )
    else:
        log.warning(
            "Fallback coef_prom для category_id=%d не найден в CSV — "
            "категории без комиссии будут пропущены",
            FALLBACK_CATEGORY_ID,
        )

    updated, fallback_used, skip_id = process_csv(CSV_PATH, commissions, fallback_coef)

    log.info(
        "=== Готово: обновлено=%d, fallback_использован=%d, невалидный_id=%d ===",
        updated, fallback_used, skip_id,
    )


if __name__ == "__main__":
    main()
