"""
market_formula_coef.py
──────────────────────
Єдине джерело правди для формули розрахунку цінового коефіцієнта маркетплейсів.

Формула:  coef = FORMULA_NUMERATOR / (100 - (PLATFORM_FEE_PERCENT + royalty_percent))

Константи та calc_coef() імпортуються всіма скриптами генерації коефіцієнтів:
  - epicenter_export_coef.py
  - kasta_export_coef.py
  - prom_export_coef.py
  - rozetka_export_coef.py

Щоб змінити числа формули — редагуй лише цей файл.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP

# ─── Константи формули ───────────────────────────────────────────────────────

FORMULA_NUMERATOR: Decimal = Decimal("110")
PLATFORM_FEE_PERCENT: Decimal = Decimal("8.5")


# ─── Спільна формула ─────────────────────────────────────────────────────────

def calc_coef(
    royalty_percent: Decimal,
    fee_percent: Decimal = PLATFORM_FEE_PERCENT,
) -> Decimal:
    """
    Обчислює ціновий коефіцієнт для заданого відсотка роялті.

    coef = FORMULA_NUMERATOR / (100 - (fee_percent + royalty_percent))

    Результат округлюється до 2 знаків після коми (ROUND_HALF_UP).

    Args:
        royalty_percent: Відсоток роялті категорії з таблиці роялті маркетплейсу.
        fee_percent:     Фіксована комісія платформи (за замовчуванням PLATFORM_FEE_PERCENT).

    Raises:
        ValueError: Якщо знаменник <= 0 (комісія + роялті >= 100 %).
    """
    denominator = Decimal("100") - (fee_percent + royalty_percent)
    if denominator <= 0:
        raise ValueError(
            f"Denominator <= 0: 100 - ({fee_percent} + {royalty_percent}) = {denominator}"
        )
    return (FORMULA_NUMERATOR / denominator).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
