"""
Епіцентр — правила ціноутворення
=================================

Логіка формування ціни (порядок кроків):
  1. Визначити retail/dealer ціни постачальника (Ціна / Оптова_ціна з *_old.csv),
     або, якщо постачальник невідомий — XML-ціну як базу (xml_fallback).
  2. Конвертувати XML-ціну у UAH, якщо currencyId ≠ UAH (тільки xml_fallback).
  3. Розрахувати ціну:
       - є category rule (threshold знайдено для категорії):
           Ціна = resolve_channel_price(retail, dealer, coef, threshold)
                = max(retail * coef, dealer * threshold)
           (див. _base.py::resolve_channel_price)
       - немає category rule (coef_uncategorized) або немає бази (coef_no_base):
           Ціна = base_price * coefficient   (як і раніше, без змін)
  4. Округлити вгору до цілої гривні (ceil_uah).
  5. Якщо отримана ціна потрапляє у діапазон
     SURCHARGE_PRICE_MIN..SURCHARGE_PRICE_MAX — додати SURCHARGE_AMOUNT.

CSV-схема коефіцієнтів (роздільник «;», кодування utf-8-sig):
  A  prom_category_id
  B  prom_category_name
  C  threshold           — авторахований коефіцієнт категорії (market_formula_coef.py),
                            застосовується до dealer як нижня межа ціни каналу
  D  coef_uncategorized  — оптова ціна є, але правило категорії відсутнє
  E  coef_no_base        — оптової ціни немає → базою слугує XML-ціна
  F  coef_viatec         — РУЧНИЙ коефіцієнт категорії для постачальника viatec (застосовується до retail).
  G  coef_secur          — РУЧНИЙ коефіцієнт категорії для постачальника secur.
  H  coef_lp             — РУЧНИЙ коефіцієнт категорії для постачальника lp.
                            Якщо для категорії є threshold, але coef_{supplier}
                            порожній для постачальника конкретного товару —
                            це КРИТИЧНА помилка (пор. apply_prices).

На відміну від Kasta, тут плоска таблиця: одне правило (threshold+coef) на
категорію, без цінових діапазонів — але коефіцієнт тепер обирається окремо
для кожного постачальника (viatec/secur/lp).
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
    ArticlePrices,
    PricingStats,
    SUPPLIERS,
    SupplierCoefficients,
    ceil_uah,
    parse_decimal,
    parse_supplier_coefficients,
    resolve_channel_price,
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
SURCHARGE_PRICE_MAX: Final[Decimal] = Decimal("3000")
SURCHARGE_AMOUNT:    Final[Decimal] = Decimal("35")


# ---------------------------------------------------------------------------
# Структури даних
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class EpicenterCategoryRule:
    """
    Одне правило категорії: авторахований поріг (threshold) + ручні коефіцієнти
    окремо для кожного постачальника (SupplierCoefficients).

    coef.get(supplier) is None → ще не заповнено вручну в epicenter_coefficients.csv
    для цього постачальника.
    """

    threshold: Decimal
    coef: SupplierCoefficients


@dataclass(frozen=True)
class EpicenterPricingTable:
    coef_uncategorized: Decimal                              # wholesale exists, no category rule → no_category_rule
    coef_no_base: Decimal                                    # no wholesale price → xml_fallback
    rules_by_category: dict[str, EpicenterCategoryRule]      # {prom_category_id: rule}


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
    rules_by_category: dict[str, EpicenterCategoryRule] = {}
    filled_coef_counts: dict[str, int] = {s: 0 for s in SUPPLIERS}

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
            raw_threshold = (row.get("threshold") or "").strip()
            if not category_id or not raw_threshold:
                continue

            try:
                threshold = parse_decimal(raw_threshold)
            except InvalidOperation:
                continue

            coef = parse_supplier_coefficients(
                row,
                context=f"Epicenter: category={category_id}",
            )
            for supplier in coef.filled_suppliers:
                filled_coef_counts[supplier] += 1

            rules_by_category[category_id] = EpicenterCategoryRule(threshold=threshold, coef=coef)

    if coef_uncategorized is None:
        raise ValueError("coef_uncategorized missing in epicenter_coefficients.csv (column D)")
    if coef_no_base is None:
        raise ValueError("coef_no_base missing in epicenter_coefficients.csv (column E)")

    filled_str = ", ".join(f"{s}={filled_coef_counts[s]}" for s in SUPPLIERS)
    print(
        f"Epicenter pricing: loaded {len(rules_by_category)} categories "
        f"(manual coef filled: {filled_str}), "
        f"coef_uncategorized={coef_uncategorized}, coef_no_base={coef_no_base}"
    )
    return EpicenterPricingTable(coef_uncategorized, coef_no_base, rules_by_category)


# ---------------------------------------------------------------------------
# Публічне API (використовується фасадом market_pricing.py)
# ---------------------------------------------------------------------------

def get_default_coefficient() -> Decimal:
    return _load_pricing().coef_uncategorized


def apply_prices(
    xml: str,
    price_index: dict[str, ArticlePrices],
    currency_rates: dict[str, Decimal],
) -> str:
    pricing = _load_pricing()
    stats = PricingStats()
    log = _build_logger()
    no_rule_offer_ids: list[str] = []
    missing_coef_categories: dict[tuple[str, str], int] = {}   # (category_id, supplier) -> кількість офферів

    def on_offer(match: re.Match) -> str:
        offer_id: str = match.group(1)
        tail_attrs: str = match.group(2)
        body: str = match.group(3)
        stats.offers += 1

        category_id = tag_text(body, "categoryId") or ""
        article = tag_text(body, "article")
        currency_id = (tag_text(body, "currencyId") or "UAH").upper()
        article_prices = price_index.get(article) if article else None

        def replace_price(price_match: re.Match) -> str:
            raw_price = price_match.group(1).strip()
            try:
                reason: str
                new_price: Decimal | None

                if article_prices is not None:
                    dealer = article_prices.dealer
                    retail = article_prices.retail
                    stats.wholesale_prices += 1

                    rule = pricing.rules_by_category.get(category_id)
                    if rule is None:
                        coefficient = pricing.coef_uncategorized
                        stats.no_category_rules += 1
                        reason = "no_category_rule"
                        no_rule_offer_ids.append(offer_id)
                        new_price = ceil_uah(dealer * coefficient)
                    else:
                        supplier = article_prices.supplier
                        supplier_coef = rule.coef.get(supplier)
                        if supplier_coef is None:
                            stats.missing_manual_coef += 1
                            key = (category_id, supplier)
                            missing_coef_categories[key] = (
                                missing_coef_categories.get(key, 0) + 1
                            )
                            reason = ""
                            new_price = None
                        else:
                            stats.category_rules += 1
                            reason = ""
                            new_price = ceil_uah(
                                resolve_channel_price(
                                    retail=retail,
                                    dealer=dealer,
                                    coef=supplier_coef,
                                    threshold=rule.threshold,
                                    logger=log,
                                    product_name=article or offer_id,
                                )
                            )
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

                if new_price is None:
                    # Критична помилка буде піднята після завершення проходу —
                    # ціну цього офера тимчасово залишаємо без змін.
                    return price_match.group(0)

                # Крок 5: надбавка після округлення
                if SURCHARGE_PRICE_MIN <= new_price <= SURCHARGE_PRICE_MAX:
                    new_price += SURCHARGE_AMOUNT

                if reason:
                    log.info(
                        "article=%-12s  offer_id=%-14s  base=%-8s  price=%-8s  reason=%s",
                        article or "—",
                        offer_id,
                        (article_prices.dealer if article_prices is not None else raw_price),
                        new_price,
                        reason,
                    )

                return f"<price>{new_price}</price>"
            except Exception as exc:
                stats.price_exceptions += 1
                log.warning(
                    "article=%-12s  offer_id=%-14s  price не змінено (виняток): %s: %s",
                    article or "—", offer_id, type(exc).__name__, exc,
                )
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
        + (f" | missing_manual_coef={stats.missing_manual_coef}" if stats.missing_manual_coef else "")
    )
    if stats.converted_prices:
        print(f"Epicenter currency conversions: {stats.converted_prices}")

    if stats.price_exceptions:
        print(f"⚠️  Epicenter: {stats.price_exceptions} оферів з винятком при розрахунку ціни (ціна з вхідного XML залишена без змін) — деталі: {DEFAULT_LOG_PATH.name}")

    errors: list[str] = []

    if stats.no_category_rules:
        # Prom автоматично перемістив товари у нові категорії без правил.
        # Фід згенеровано з coef_uncategorized — ціни некоректні.
        # Потрібно додати правила у epicenter_coefficients.csv.
        # Деталі у epicenter_default_id.log
        ids_str = ", ".join(no_rule_offer_ids)
        errors.append(
            f"{stats.no_category_rules} товарів без правил категорії (no_category_rules). "
            f"Додайте правила у epicenter_coefficients.csv. Деталі: epicenter_default_id.log\n"
            f"Offer IDs: {ids_str}"
        )

    if missing_coef_categories:
        details = "\n".join(
            f"  category_id={cat_id}  supplier={supplier}  ({count} офферів)"
            for (cat_id, supplier), count in sorted(missing_coef_categories.items())
        )
        errors.append(
            f"{stats.missing_manual_coef} офферів мають threshold, але ручний "
            f"coef_{{supplier}} не заповнений у epicenter_coefficients.csv. Заповніть коефіцієнт для цих "
            f"категорій/постачальників перед генерацією фіду:\n{details}"
        )

    if errors:
        raise SystemExit("❌ Epicenter:\n\n" + "\n\n".join(errors))

    return updated_xml
