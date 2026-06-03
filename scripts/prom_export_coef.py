"""
prom_export_coef.py
───────────────────
Заполняет три файла на основе данных из Google Sheets (gid=688167001).

Таблица открыта по ссылке без авторизации — используем CSV export.

Источник (Google Sheets):
  Столбик G  → ID категорії   (prom_category_id)
  Столбик I  → Комісія для режиму «Економ»  (X, %)

Алгоритм:
  1. Загружаем Google Sheets → dict[category_id → commission_percent]
  2. Fallback — coef_transition из строки дефолтов prom_coefficients.csv
     (строка с пустым prom_category_id, не из Google Sheets)
  3. prom_coefficients.csv:
       ключ: prom_category_id  →  заполняем: coef_cry
  4. viatec_category.csv  (только строки channel == "prom"):
       ключ: Ідентифікатор_підрозділу  →  заполняем: threshold
  5. secur_category.csv   (только строки channel == "prom"):
       ключ: Ідентифікатор_підрозділу  →  заполняем: threshold

  Если category_id не найден в Google Sheets → fallback coef_transition.
  Строки дефолтов (пустой ключ) и строки channel != "prom" — не трогаем.

Запуск:
    python scripts/prom_export_coef.py
"""

from __future__ import annotations

import csv
import io
import logging
import sys
from decimal import Decimal
from pathlib import Path

import requests

from services.market_formula_coef import calc_coef

# ─────────────────────────────── config ───────────────────────────────────────

BASE_DIR    = Path(r"C:\FullStack\PriceFeedPipeline\data")
MARKET_DIR  = BASE_DIR / "markets"

PROM_COEF_CSV  = MARKET_DIR / "prom_coefficients.csv"
VIATEC_CAT_CSV = BASE_DIR / "viatec" / "viatec_category.csv"
SECUR_CAT_CSV  = BASE_DIR / "secur"  / "secur_category.csv"

SPREADSHEET_ID    = "1mQ86nxmPTsEj23MAAu4bGn4iKtaX-yEIiKefu4SqvkA"
SHEET_GID         = "688167001"
SHEETS_EXPORT_URL = (
    f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}"
    f"/export?format=csv&gid={SHEET_GID}"
)

# Индексы столбцов в Google Sheets (0-based): A=0 … G=6 … I=8
SHEETS_COL_CATEGORY_ID = 6   # G — ID категорії
SHEETS_COL_COMMISSION  = 8   # I — Комісія для режиму «Економ»

# Строка, с которой начинаются данные (1-based, строка 1 — заголовок)
SHEETS_DATA_START_ROW = 2

# ── prom_coefficients.csv ──────────────────────────────────────────────────────
PROM_COL_CAT_ID          = "prom_category_id"
PROM_COL_COEF            = "coef_cry"         # заполняем
PROM_COL_COEF_TRANSITION = "coef_transition"  # источник fallback-значения

# Строка дефолтов — пустой prom_category_id. Её coef_transition используется
# как fallback для всех трёх файлов. Скрипт эту строку не трогает.

# ── category CSV (viatec / secur) ─────────────────────────────────────────────
CAT_COL_CHANNEL   = "channel"
CAT_COL_KEY       = "Ідентифікатор_підрозділу"  # ключ поиска
CAT_COL_THRESHOLD = "threshold"                  # заполняем
CAT_CHANNEL_PROM  = "prom"

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

def load_prom_commissions(url: str) -> dict[int, float]:  # noqa: C901
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
    Читает coef_transition из строки дефолтов prom_coefficients.csv
    (строка с пустым prom_category_id). Используется как fallback для всех файлов.
    """
    raw    = csv_path.read_text(encoding=CSV_ENCODING)
    reader = csv.DictReader(io.StringIO(raw), delimiter=CSV_DELIMITER)

    if reader.fieldnames is None or PROM_COL_COEF_TRANSITION not in reader.fieldnames:
        return None

    for row in reader:
        if not row.get(PROM_COL_CAT_ID, "").strip():  # строка дефолтов
            coef = row.get(PROM_COL_COEF_TRANSITION, "").strip()
            return coef if coef else None

    return None


# ─────────────────────────────── CSV utils ────────────────────────────────────

def _ensure_column(fieldnames: list[str], col: str) -> list[str]:
    """Добавляет col в fieldnames если отсутствует."""
    if col not in fieldnames:
        fieldnames = list(fieldnames) + [col]
        log.info("Столбик '%s' добавлен в CSV", col)
    return fieldnames


def _write_csv(csv_path: Path, fieldnames: list[str], rows: list[dict]) -> None:
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


def _resolve_coef(
    cat_id: int,
    commissions: dict[int, float],
    fallback_coef: str | None,
    label: str,
) -> tuple[str | None, bool]:
    """
    Возвращает (coef_str, is_fallback):
      - найден в Sheets → считаем по формуле, is_fallback=False
      - не найден       → fallback_coef,      is_fallback=True
      - нет fallback    → None,                is_fallback=False
    """
    commission = commissions.get(cat_id)

    if commission is not None:
        try:
            coef_str = str(calc_coef(Decimal(str(commission))))
        except ValueError as exc:
            log.error("%s category_id=%d: ошибка расчёта: %s — пропускаем", label, cat_id, exc)
            return None, False
        log.info("%s category_id=%-12d  commission=%-6.2f%%  coef=%s", label, cat_id, commission, coef_str)
        return coef_str, False

    if fallback_coef is None:
        log.warning(
            "%s category_id=%d: не найден в Google Sheets, fallback не задан — пропускаем",
            label, cat_id,
        )
        return None, False

    log.warning(
        "%s category_id=%d: не найден в Google Sheets → fallback coef_transition=%s",
        label, cat_id, fallback_coef,
    )
    return fallback_coef, True


# ─────────────────────────────── CSV processing ───────────────────────────────

def process_prom_coef_csv(
    csv_path: Path,
    commissions: dict[int, float],
    fallback_coef: str | None,
) -> tuple[int, int, int]:
    """
    Заполняет coef_cry в prom_coefficients.csv.
    Ключ: prom_category_id. Строку дефолтов не трогаем.
    Возвращает (updated, fallback_used, skipped).
    """
    raw    = csv_path.read_text(encoding=CSV_ENCODING)
    reader = csv.DictReader(io.StringIO(raw), delimiter=CSV_DELIMITER)

    if reader.fieldnames is None:
        raise RuntimeError(f"CSV {csv_path} пуст или не читается")
    if PROM_COL_CAT_ID not in reader.fieldnames:
        raise RuntimeError(f"Столбик '{PROM_COL_CAT_ID}' не найден в {csv_path}")

    fieldnames = _ensure_column(list(reader.fieldnames), PROM_COL_COEF)
    rows       = list(reader)

    updated, fallback_used, skipped = 0, 0, 0

    for row in rows:
        raw_id = row.get(PROM_COL_CAT_ID, "").strip()
        if not raw_id:
            continue  # строка дефолтов — не трогаем

        try:
            cat_id = int(raw_id)
        except (ValueError, TypeError):
            log.warning("prom_coef: невалидный prom_category_id '%s' — пропускаем", raw_id)
            skipped += 1
            continue

        coef_str, is_fallback = _resolve_coef(cat_id, commissions, fallback_coef, "[prom_coef]")
        if coef_str is None:
            skipped += 1
            continue

        row[PROM_COL_COEF] = coef_str
        updated += 1
        if is_fallback:
            fallback_used += 1

    _write_csv(csv_path, fieldnames, rows)
    return updated, fallback_used, skipped


def process_category_csv(
    csv_path: Path,
    commissions: dict[int, float],
    fallback_coef: str | None,
    label: str,
) -> tuple[int, int, int]:
    """
    Заполняет threshold в category CSV (viatec / secur).
    Фильтр: channel == "prom".
    Ключ: Ідентифікатор_підрозділу.
    Строки с пустым ключом или channel != "prom" — не трогаем.
    Возвращает (updated, fallback_used, skipped).
    """
    raw    = csv_path.read_text(encoding=CSV_ENCODING)
    reader = csv.DictReader(io.StringIO(raw), delimiter=CSV_DELIMITER)

    if reader.fieldnames is None:
        raise RuntimeError(f"CSV {csv_path} пуст или не читается")
    for required in (CAT_COL_CHANNEL, CAT_COL_KEY):
        if required not in reader.fieldnames:
            raise RuntimeError(f"Столбик '{required}' не найден в {csv_path}")

    fieldnames = _ensure_column(list(reader.fieldnames), CAT_COL_THRESHOLD)
    rows       = list(reader)

    updated, fallback_used, skipped = 0, 0, 0

    for row in rows:
        if row.get(CAT_COL_CHANNEL, "").strip() != CAT_CHANNEL_PROM:
            continue  # только prom-строки

        raw_id = row.get(CAT_COL_KEY, "").strip()
        if not raw_id:
            continue  # нет ключа — не трогаем

        try:
            cat_id = int(raw_id)
        except (ValueError, TypeError):
            log.warning("%s: невалидный %s='%s' — пропускаем", label, CAT_COL_KEY, raw_id)
            skipped += 1
            continue

        coef_str, is_fallback = _resolve_coef(cat_id, commissions, fallback_coef, f"[{label}]")
        if coef_str is None:
            skipped += 1
            continue

        row[CAT_COL_THRESHOLD] = coef_str
        updated += 1
        if is_fallback:
            fallback_used += 1

    _write_csv(csv_path, fieldnames, rows)
    return updated, fallback_used, skipped


# ─────────────────────────────── main ─────────────────────────────────────────

def main() -> None:
    log.info("=== prom_export_coef старт ===")

    missing = [p for p in (PROM_COEF_CSV, VIATEC_CAT_CSV, SECUR_CAT_CSV) if not p.exists()]
    if missing:
        for p in missing:
            log.error("Файл не найден: %s", p)
        sys.exit(1)

    try:
        commissions = load_prom_commissions(SHEETS_EXPORT_URL)
    except RuntimeError as exc:
        log.error("%s", exc)
        sys.exit(1)

    if not commissions:
        log.error("Google Sheets: нет валидных данных — прерываем")
        sys.exit(1)

    # Fallback читаем из prom_coefficients.csv — единственный источник для всех файлов
    fallback_coef = read_fallback_coef(PROM_COEF_CSV)
    if fallback_coef:
        log.info("Fallback coef_transition=%s (строка дефолтов prom_coefficients.csv)", fallback_coef)
    else:
        log.warning("Fallback coef_transition не найден — категории без комиссии будут пропущены")

    # prom_coefficients.csv → coef_cry
    log.info("--- %s ---", PROM_COEF_CSV.name)
    upd, fb, skip = process_prom_coef_csv(PROM_COEF_CSV, commissions, fallback_coef)
    log.info("[prom_coef]  обновлено=%d  fallback=%d  пропущено=%d", upd, fb, skip)

    # viatec_category.csv → threshold (channel==prom)
    log.info("--- %s ---", VIATEC_CAT_CSV.name)
    upd, fb, skip = process_category_csv(VIATEC_CAT_CSV, commissions, fallback_coef, "viatec")
    log.info("[viatec]     обновлено=%d  fallback=%d  пропущено=%d", upd, fb, skip)

    # secur_category.csv → threshold (channel==prom)
    log.info("--- %s ---", SECUR_CAT_CSV.name)
    upd, fb, skip = process_category_csv(SECUR_CAT_CSV, commissions, fallback_coef, "secur")
    log.info("[secur]      обновлено=%d  fallback=%d  пропущено=%d", upd, fb, skip)

    log.info("=== prom_export_coef завершён ===")


if __name__ == "__main__":
    main()
