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

  Друга проблема: НОВІ категорії (яких раніше не було в CSV, отже немає
  ручного override) отримували порожні coef_viatec/coef_secur/coef_lp. Тепер
  вони заповнюються дефолтом з рядка дефолтів (порожній prom_category_id,
  перший рядок даних CSV) — того самого рядка, що вже містить
  coef_uncategorized/coef_no_base. Ручний override (read_manual_overrides)
  завжди має пріоритет над цим дефолтом.

Використання (у кожному {market}_export_coef.py):
    from services.coef_export_service import (
        SUPPLIER_COEF_FIELDS,
        read_defaults_row,
        read_manual_overrides,
    )

    supplier_defaults = read_defaults_row(CSV_PATH, SUPPLIER_COEF_FIELDS)
    overrides = read_manual_overrides(
        CSV_PATH,
        key_fields=("prom_category_id",),          # унікальний ключ рядка для цього маркетплейсу
        preserve_fields=SUPPLIER_COEF_FIELDS,       # однаково для epicenter/kasta/rozetka
    )
    ...
    row.update(supplier_defaults)                   # базові coef_viatec/secur/lp для НОВИХ рядків
    ...                                              # (побудова решти полів рядка)
    row.update(overrides.get(row_key, {}))          # ручний override — застосувати ОСТАННІМ (пріоритет)

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


def read_defaults_row(
    csv_path: Path,
    fields: tuple[str, ...],
    key_field: str = "prom_category_id",
) -> dict[str, str]:
    """
    Читає рядок дефолтів із {csv_path} — перший рядок даних з порожнім
    key_field (зазвичай prom_category_id) — і повертає {поле: значення} для всіх
    fields з непорожнім значенням в цьому рядку.

    Рядок дефолтів — єдине джерело правди для базових значень coef_viatec/
    coef_secur/coef_lp для НОВИХ категорій, які ще не мають власного ручного override у
    read_manual_overrides. Застосовувати РАНІШЕ read_manual_overrides при побудові
    рядка — ручний override повинен мати пріоритет над цим дефолтом.

    Знаходить саме перший рядок з порожнім key_field (інші рядки з порожнім
    key_field б теоретично бути помилкою даних і ігноруються).

    Повертає {} якщо файл не існує, порожній, без рядка дефолтів, або в цьому
    рядку жодене з fields не заповнене.
    """
    if not csv_path.exists():
        return {}

    with csv_path.open(encoding=CSV_ENCODING, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=CSV_DELIMITER)
        if not reader.fieldnames or key_field not in reader.fieldnames:
            return {}

        available_fields = tuple(field for field in fields if field in reader.fieldnames)
        if not available_fields:
            return {}

        for row in reader:
            if (row.get(key_field) or "").strip():
                continue  # не рядок дефолтів — пропускаємо
            return {
                field: value
                for field in available_fields
                if (value := (row.get(field) or "").strip())
            }

    return {}
