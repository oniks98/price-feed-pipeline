"""
coef_export_service.py
───────────────────────
Єдине джерело правди для ідемпотентного збереження вручну заповнених
стовпців коефіцієнтів при регенерації *_coefficients.csv.

Проблема, яку вирішує цей модуль:
  {market}_export_coef.py (epicenter/kasta/rozetka) при кожному запуску
  ПОВНІСТЮ перебудовує рядки з мапінгу/роялті і перезаписує CSV. Без явного
  перенесення попередніх значень будь-який вручну заповнений коефіцієнт
  постачальника (coef_viatec / coef_secur / coef_lp) губиться на наступному
  запуску скрипта. Однаково для epicenter, kasta і rozetka — без винятків.

Використання (у кожному {market}_export_coef.py):
    from services.coef_export_service import SUPPLIER_COEF_FIELDS, read_manual_overrides

    overrides = read_manual_overrides(
        CSV_PATH,
        key_fields=("prom_category_id",),          # унікальний ключ рядка для цього маркетплейсу
        preserve_fields=SUPPLIER_COEF_FIELDS,       # однаково для epicenter/kasta/rozetka
    )
    ...
    row.update(overrides.get(row_key, {}))          # застосувати ПІСЛЯ побудови базового рядка

Ключ рядка (key_fields) залежить від гранулярності правил маркетплейсу:
  - epicenter: (prom_category_id,)                              — одне правило на категорію
  - kasta:     (prom_category_id, price_from, price_to)         — правило на ціновий діапазон
  - rozetka:   (prom_category_id, brand, price_from, price_to)  — правило на бренд+діапазон
"""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Final

from services.pricing_rules import SUPPLIERS

# ─── Конфіг CSV ──────────────────────────────────────────────────────────────

CSV_DELIMITER: Final[str] = ";"
CSV_ENCODING: Final[str] = "utf-8-sig"

# Стовпці ручних коефіцієнтів постачальників — єдине джерело правди SUPPLIERS
# (services/pricing_rules/_base.py). Новий постачальник автоматично підхоплюється
# усіма {market}_export_coef.py без правок тут.
SUPPLIER_COEF_FIELDS: Final[tuple[str, ...]] = tuple(f"coef_{supplier}" for supplier in SUPPLIERS)


# ─── Публічне API ────────────────────────────────────────────────────────────

def read_manual_overrides(
    csv_path: Path,
    key_fields: tuple[str, ...],
    preserve_fields: tuple[str, ...],
) -> dict[tuple[str, ...], dict[str, str]]:
    """
    Читає {csv_path} (якщо файл ще не існує — повертає {}) і повертає
    {ключ_рядка: {поле: значення}} для всіх preserve_fields з непорожнім значенням.

    Ключ рядка — кортеж значень key_fields (у тому ж порядку), обрізаних пробілів.
    Рядок дефолтів (порожнє перше поле ключа, зазвичай prom_category_id) пропускається —
    це не ручний override, а глобальні дефолти (coef_uncategorized/coef_no_base).

    preserve_fields, яких немає серед стовпців CSV, тихо ігноруються — це дозволяє
    викликати функцію ще до того, як стовпець зʼявився у файлі (напр. перший запуск
    після додавання нового постачальника).
    """
    if not csv_path.exists():
        return {}

    overrides: dict[tuple[str, ...], dict[str, str]] = {}

    with csv_path.open(encoding=CSV_ENCODING, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        if not reader.fieldnames:
            return {}

        available_fields = tuple(field for field in preserve_fields if field in reader.fieldnames)
        if not available_fields:
            return {}

        for row in reader:
            key = tuple((row.get(field) or "").strip() for field in key_fields)
            if not key[0]:
                continue  # рядок дефолтів — не override

            values = {
                field: value
                for field in available_fields
                if (value := (row.get(field) or "").strip())
            }
            if values:
                overrides[key] = values

    return overrides
