# suppliers/constants.py

from __future__ import annotations

from pathlib import Path
from typing import Final, Mapping

"""
Константи для модуля suppliers.
Централізоване зберігання налаштувань для всіх постачальників.

Документація: C:/FullStack/Scrapy/ADD_NEW_SUPPLIER.md
Тестування:   python test_price_rounding.py
"""

# =============================================================================
# БАЗОВІ ШЛЯХИ
# =============================================================================
# Підтримує локальний запуск (дефолт) і GitHub Actions (через env PROJECT_ROOT)
import os as _os
_PROJECT_ROOT: Final[Path] = Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy"))

BASE_DATA_DIR: Final[Path] = _PROJECT_ROOT / "data"
OUTPUT_DIR: Final[Path] = BASE_DATA_DIR / "output"

# =============================================================================
# ОКРУГЛЕННЯ ЦІН
# =============================================================================
# Кількість десяткових знаків після коми для кожного постачальника.
#
# Значення:
#   0 = цілі числа (UAH):        1234.56 × 1.15 → 1420
#   2 = копійки (USD, EUR):      123.456 × 1.25 → 154.32
#   3 = 3 знаки (рідкісні випадки)
#
# Приклади:
#   viatec_dealer: 2  → USD з копійками (123.45)
#   viatec_retail: 0  → UAH цілі (1235)
#   eserver: 0        → UAH цілі (1235)
# =============================================================================

DEFAULT_PRICE_DECIMALS: Final[int] = 0  # UAH за замовчуванням

PRICE_DECIMALS: Final[Mapping[str, int]] = {
    # USD постачальники (ціни з копійками)
    "viatec_dealer": 2,  # USD з копійками
    # UAH постачальники (ціни цілі)
    "viatec_retail": 0,  # UAH цілі
    "eserver": 0,
    "secur": 0,
    "lun": 0,
}

# =============================================================================
# СТАРТОВІ КОДИ ТОВАРІВ
# =============================================================================
# Кожен постачальник має свій діапазон Код_товару (по 100 000 на постачальника).
# Реальний код прив'язується до SKU через data/{supplier}/sku_map.json.
# Файли *_counter_product_code.csv більше не потрібні.
# =============================================================================

SUPPLIER_CODE_RANGES: Final[Mapping[str, int]] = {
    "viatec":   200000,
    "secur":    100100,
    "eserver":  600000,
    "neolight": 500000,
    "lun":      401001,
}

DEFAULT_START_CODE: Final[int] = 200000

# =============================================================================
# ВАЛЮТИ ПОСТАЧАЛЬНИКІВ
# =============================================================================
# Мапінг постачальник → валюта (для довідки).
# Використовується в spider для встановлення поля "Валюта".
# =============================================================================

DEFAULT_CURRENCY: Final[str] = "UAH"

SUPPLIER_CURRENCIES: Final[Mapping[str, str]] = {
    "viatec_dealer": "USD",
    "viatec_retail": "UAH",
    "eserver": "UAH",
    "secur": "UAH",
    "lun": "UAH",
}

# =============================================================================
# ХЕЛПЕРИ
# =============================================================================


def get_price_decimals(supplier_name: str) -> int:
    """
    Отримує кількість десяткових знаків для постачальника.

    Args:
        supplier_name: Назва постачальника (viatec, eserver, тощо)

    Returns:
        Кількість десяткових знаків (0, 2, тощо)
    """
    key = supplier_name.strip().lower()
    return int(PRICE_DECIMALS.get(key, DEFAULT_PRICE_DECIMALS))


def get_start_code(supplier_name: str) -> int:
    """
    Отримує стартовий Код_товару для постачальника.
    Використовується тільки при першому запуску (коли sku_map.json ще не існує).

    Args:
        supplier_name: Базова назва постачальника (viatec, secur, тощо)

    Returns:
        Стартовий числовий код
    """
    key = supplier_name.strip().lower().split("_")[0]  # viatec_dealer → viatec
    return int(SUPPLIER_CODE_RANGES.get(key, DEFAULT_START_CODE))


def get_currency(supplier_name: str) -> str:
    """
    Отримує валюту постачальника.

    Args:
        supplier_name: Назва постачальника

    Returns:
        Код валюти (USD, UAH, тощо)
    """
    key = supplier_name.strip().lower()
    return str(SUPPLIER_CURRENCIES.get(key, DEFAULT_CURRENCY))


__all__ = [
    "BASE_DATA_DIR",
    "OUTPUT_DIR",
    "DEFAULT_PRICE_DECIMALS",
    "PRICE_DECIMALS",
    "DEFAULT_CURRENCY",
    "SUPPLIER_CURRENCIES",
    "SUPPLIER_CODE_RANGES",
    "DEFAULT_START_CODE",
    "get_price_decimals",
    "get_currency",
    "get_start_code",
]
