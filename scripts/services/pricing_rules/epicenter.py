"""
Епіцентр — правила ціноутворення
=================================

Логіка формування ціни (порядок кроків):
  1. Визначити базову ціну (оптова або XML-ціна).
  2. Конвертувати у UAH, якщо currencyId ≠ UAH.
  3. Помножити на коефіцієнт категорії (або на запасний коефіцієнт).
  4. Округлити вгору до цілої гривні (ceil_uah).
  5. Якщо отримана ціна потрапляє у діапазон
     SURCHARGE_PRICE_MIN..SURCHARGE_PRICE_MAX — додати SURCHARGE_AMOUNT.

CSV-схема коефіцієнтів (роздільник «;», кодування utf-8-sig):
  A  prom_category_id
  B  prom_category_name
  C  coef                — коефіцієнт категорії (застосовується до оптової ціни)
  D  coef_uncategorized  — оптова ціна є, але правило категорії відсутнє
  E  coef_no_base        — оптової ціни немає → базою слугує XML-ціна

На відміну від Kasta, тут плоска таблиця: один коефіцієнт на категорію,
без цінових діапазонів.
"""

from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Final

from ._base import (
    PricingStats,
    ceil_uah,
    parse_decimal,
    tag_text,
)

# ---------------------------------------------------------------------------
# Шляхи до файлів
# ---------------------------------------------------------------------------

_ROOT: Final[Path] = Path(__file__).resolve().parents[3]
COEFFICIENTS_PATH: Final[Path] = _ROOT / "data" / "markets" / "epicenter_coefficients.csv"
DEFAULT_LOG_PATH: Final[Path] = _ROOT / "epicenter_default_id.log"
_CSV_DELIMITER: Final[str] = ";"
_CSV_ENCODING: Final[str] = "utf-8-sig"


# ---------------------------------------------------------------------------
# Надбавка до ціни після множення на коефіцієнт
# Застосовується якщо ціна ∈ [SURCHARGE_PRICE_MIN, SURCHARGE_PRICE_MAX]
# ---------------------------------------------------------------------------

SURCHARGE_PRICE_MIN: Final[Decimal] = Decimal("199")
SURCHARGE_PRICE_MAX: Final[Decimal] = Decimal("1000")
SURCHARGE_AMOUNT:    Final[Decimal] = Decimal("35")


# ---------------------------------------------------------------------------
# Структури даних
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EpicenterPricingTable:
    coef_uncategorized: Decimal                    # wholesale exists, no category rule → no_category_rule
    coef_no_base: Decimal                          # no wholesale price → xml_fallback
    coef_by_category: dict[str, Decimal]           # {prom_category_id: coef}


# ---------------------------------------------------------------------------
# Логер
# ---------------------------------------------------------------------------

def _build_logger() -> logging.Logger:
    """
    Logger that writes to epicenter_default_id.log.
    Overwrites on every run (mode='w') so the log always matches the current feed.
    """
    logger = logging.getLogger("epicenter.default_offers")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    handler = logging.FileHandler(DEFAULT_LOG_PATH, mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


# ---------------------------------------------------------------------------
# Завантаження CSV-коефіцієнтів
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_pricing() -> EpicenterPricingTable:
    if not COEFFICIENTS_PATH.exists():
        raise FileNotFoundError(f"Epicenter coefficients not found: {COEFFICIENTS_PATH}")

    coef_uncategorized: Decimal | None = None
    coef_no_base: Decimal | None = None
    coef_by_category: dict[str, Decimal] = {}

    with COEFFICIENTS_PATH.open(encoding=_CSV_ENCODING, errors="replace", newline="") as f:
        reader = csv.DictReader(f, delimiter=_CSV_DELIMITER)
        for row in reader:
            if coef_uncategorized is None:
                raw = (row.get("coef_uncategorized") or "").strip()
                if raw:
                    coef_uncategorized = parse_decimal(raw)

            if coef_no_base is None:
                raw = (row.get("coef_no_base") or "").strip()
                if raw:
                    coef_no_base = parse_decimal(raw)

            category_id = (row.get("prom_category_id") or "").strip().strip("\ufeff")
            raw_coef = (row.get("coef") or "").strip()
            if not category_id or not raw_coef:
                continue

            try:
                coef_by_category[category_id] = parse_decimal(raw_coef)
            except InvalidOperation:
                continue

    if coef_uncategorized is None:
        raise ValueError("coef_uncategorized missing in epicenter_coefficients.csv (column D)")
    if coef_no_base is None:
        raise ValueError("coef_no_base missing in epicenter_coefficients.csv (column E)")

    print(
        f"Epicenter pricing: loaded {len(coef_by_category)} categories, "
        f"coef_uncategorized={coef_uncategorized}, coef_no_base={coef_no_base}"
    )
    return EpicenterPricingTable(coef_uncategorized, coef_no_base, coef_by_category)


# ---------------------------------------------------------------------------
# Публічне API (використовується фасадом market_pricing.py)
# ---------------------------------------------------------------------------

def get_default_coefficient() -> Decimal:
    return _load_pricing().coef_uncategorized


def apply_prices(
    xml: str,
    wholesale_index: dict[str, Decimal],
    currency_rates: dict[str, Decimal],
) -> str:
    pricing = _load_pricing()
    stats = PricingStats()
    log = _build_logger()

    def on_offer(match: re.Match) -> str:
        offer_id: str = match.group(1)
        tail_attrs: str = match.group(2)
        body: str = match.group(3)
        stats.offers += 1

        category_id = tag_text(body, "categoryId") or ""
        article = tag_text(body, "article")
        currency_id = (tag_text(body, "currencyId") or "UAH").upper()
        wholesale_price = wholesale_index.get(article) if article else None

        def replace_price(price_match: re.Match) -> str:
            raw_price = price_match.group(1).strip()
            try:
                reason: str
                if wholesale_price is not None:
                    base_price = wholesale_price
                    stats.wholesale_prices += 1

                    category_coef = pricing.coef_by_category.get(category_id)
                    if category_coef is None:
                        coefficient = pricing.coef_uncategorized
                        stats.no_category_rules += 1
                        reason = "no_category_rule"
                    else:
                        coefficient = category_coef
                        stats.category_rules += 1
                        reason = ""
                else:
                    base_price = parse_decimal(raw_price)
                    if currency_id != "UAH":
                        rate = currency_rates.get(currency_id)
                        if rate is None:
                            print(
                                f"Currency rate for {currency_id} not found, "
                                f"offer {offer_id}: price left unconverted"
                            )
                        else:
                            base_price *= rate
                            stats.converted_prices += 1

                    coefficient = pricing.coef_no_base
                    stats.xml_fallback_prices += 1
                    reason = "xml_fallback"

                new_price = ceil_uah(base_price * coefficient)

                # Крок 5: надбавка після округлення
                if SURCHARGE_PRICE_MIN <= new_price <= SURCHARGE_PRICE_MAX:
                    new_price += SURCHARGE_AMOUNT

                if reason:
                    log.info(
                        "article=%-12s  offer_id=%-14s  base=%-8s  coef=%s  price=%-8s  reason=%s",
                        article or "—",
                        offer_id,
                        base_price,
                        coefficient,
                        new_price,
                        reason,
                    )

                return f"<price>{new_price}</price>"
            except Exception:
                return price_match.group(0)

        new_body = re.sub(r"<price>(.*?)</price>", replace_price, body)
        new_body = re.sub(
            r"<currencyId>[^<]+</currencyId>",
            "<currencyId>UAH</currencyId>",
            new_body,
        )
        return f'<offer id="{offer_id}"{tail_attrs}>{new_body}</offer>'

    updated_xml = re.sub(
        r'<offer\s+id="(\d+)"([^>]*)>(.*?)</offer>',
        on_offer,
        xml,
        flags=re.DOTALL,
    )

    print(f"Epicenter offers: {stats.offers}")
    print(
        "Epicenter coefficients: "
        f"category_rules={stats.category_rules} | "
        f"xml_fallback={stats.xml_fallback_prices} | "
        f"no_category_rules={stats.no_category_rules}"
    )
    if stats.converted_prices:
        print(f"Epicenter currency conversions: {stats.converted_prices}")

    if stats.no_category_rules:
        # Prom автоматично перемістив товари у нові категорії без правил.
        # Фід згенеровано з coef_uncategorized — ціни некоректні.
        # Потрібно додати правила у epicenter_coefficients.csv.
        # Деталі у epicenter_default_id.log
        raise SystemExit(
            f"\u274c Epicenter: {stats.no_category_rules} \u0442\u043e\u0432\u0430\u0440\u0456\u0432 \u0431\u0435\u0437 \u043f\u0440\u0430\u0432\u0438\u043b \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u0457 "
            f"(no_category_rules). \u0414\u043e\u0434\u0430\u0439\u0442\u0435 \u043f\u0440\u0430\u0432\u0438\u043b\u0430 \u0443 epicenter_coefficients.csv. "
            f"\u0414\u0435\u0442\u0430\u043b\u0456: epicenter_default_id.log"
        )

    return updated_xml
