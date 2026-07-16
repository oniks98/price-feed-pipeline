"""
Fallback-розрахунок поля "Кількість" для категорій, де Rozetka (через фід,
що агрегується з Prom-виводу по каналу "site") вимагає правдоподібну
кількість, а постачальник не дає точних даних, доки залишок >= ~10 од.
До того часу availability_service.py / lp_quickproduction_service.py (та
аналогічні сервіси інших постачальників) пишуть плейсхолдер PLACEHOLDER_QTY.

Конфіг (категорії + цінові тіри) — data/markets/rozetka_fallback_qty.csv.
Ключ — Ідентифікатор_підрозділу (категорія Prom/Rozetka), НЕ постачальник:
той самий файл однаково працює для lp/secur/viatec/eserver — хто б не
привіз товар у категорію 5280501/14191106/500901. Список категорій і меж
росте із часом і редагується без деплою — тому CSV, а не хардкод.

Детермінізм: seed = md5(Код_товару) → те саме число для того самого товару
при кожному запуску пайплайна (idempotent, resume-safe). random.Random(seed)
створює незалежний генератор, не займає глобальний random module —
parallel-safe.

Сервіс нічого не знає про канали (це рішення pipelines.py, дивись коментар
біля виклику — тільки channel="site") і нічого не логує (single
responsibility, легко тестується без mock-логера) — повертає (qty, reason);
логування — відповідальність виклика.
"""
from __future__ import annotations

import csv
import hashlib
import random
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from suppliers.constants import BASE_DATA_DIR, PLACEHOLDER_QTY

__all__ = ["resolve_fallback_qty"]

_CONFIG_FILE: Path = BASE_DATA_DIR / "markets" / "rozetka_fallback_qty.csv"


@dataclass(frozen=True, slots=True)
class PriceBand:
    min_price: float          # включно
    max_price: float | None   # виключно; None = без верхньої межі
    qty_min: int
    qty_max: int


@lru_cache(maxsize=1)
def _load_rules() -> dict[str, list[PriceBand]]:
    """Читається один раз за запуск пайплайна (кеш процесу)."""
    rules: dict[str, list[PriceBand]] = {}
    with _CONFIG_FILE.open(encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f, delimiter=";"):
            cat = row["Ідентифікатор_підрозділу"].strip()
            band = PriceBand(
                min_price=float(row["price_min"]),
                max_price=float(row["price_max"]) if row["price_max"].strip() else None,
                qty_min=int(row["qty_min"]),
                qty_max=int(row["qty_max"]),
            )
            rules.setdefault(cat, []).append(band)
    return rules


def _find_band(bands: list[PriceBand], price: float) -> PriceBand | None:
    """Бендів мало (кілька на категорію) — лінійний пошук, bisect не потрібен."""
    for band in bands:
        if price >= band.min_price and (band.max_price is None or price < band.max_price):
            return band
    return None


def resolve_fallback_qty(
    *,
    item_id: str,
    subdivision_id: str,
    price: str,
    qty: str,
) -> tuple[str, str]:
    """
    Args:
        item_id:        cleaned["Код_товару"] — стабільний seed
        subdivision_id: cleaned["Ідентифікатор_підрозділу"]
        price:          cleaned["Ціна"] (рядок, UAH, може бути "")
        qty:            cleaned["Кількість"] (поточне значення з пайплайна)

    Returns:
        (qty, reason). reason ∈ {
            "not_configured"  — категорія не в rozetka_fallback_qty.csv
            "not_placeholder" — qty вже реальне значення від постачальника
            "invalid_price"   — ціна порожня/не парситься (аномалія)
            "no_band_match"   — ціна поза всіма бендами категорії (аномалія)
            "applied"         — фоллбек застосовано
        }
    """
    bands = _load_rules().get(str(subdivision_id))
    if bands is None:
        return qty, "not_configured"

    if str(qty) != PLACEHOLDER_QTY:
        return qty, "not_placeholder"

    try:
        price_val = float(str(price).strip().replace(",", "."))
    except (ValueError, TypeError):
        return qty, "invalid_price"

    band = _find_band(bands, price_val)
    if band is None:
        return qty, "no_band_match"

    seed = int(hashlib.md5(str(item_id).encode()).hexdigest(), 16)
    resolved = random.Random(seed).randint(band.qty_min, band.qty_max)

    return str(resolved), "applied"
