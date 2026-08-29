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
  5. Лише після кроків 1–4 (усіх коефіцієнтів із CSV та округлення) для ціни
     у діапазоні 250..30 000 грн визначити найбільшу з фактичної
     та об'ємної ваги (L × W × H / 4 000), якщо всі сторони ≤ 70 см:
        < 2 кг → +90 грн, < 10 кг → +135 грн, ≤ 30 кг → +200 грн.
     Якщо є фактична вага, але габарити неповні — використати фактичну вагу
     (за умови, що жодна відома сторона не перевищує 70 см). Якщо немає ані
     ваги, ані габаритів — для ціни до 6 000 грн додати fallback +100 грн та
     вивести ID офера у зведенні.

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
from html import unescape
import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Callable, Final

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
DEFAULT_LOG_PATH: Final[Path] = _ROOT / "logs" / "epicenter_default_id.log"
DEFAULT_MARKUP_LOG_PATH: Final[Path] = _ROOT / "logs" / "epicenter_default_markup_id.log"
_CSV_DELIMITER: Final[str] = ";"
_CSV_ENCODING: Final[str] = "utf-8-sig"


# ---------------------------------------------------------------------------
# Надбавка до ціни після множення на коефіцієнт.
#
# Відправлення допускається лише тоді, коли розрахована ціна, фактична/
# об'ємна вага та всі габарити вкладаються в межі нижче.  Об'ємна вага
# рахується за габаритами упаковки, бо саме вона визначає тариф доставки.
# ---------------------------------------------------------------------------

SURCHARGE_PRICE_MIN: Final[Decimal] = Decimal("200")
SURCHARGE_PRICE_MAX: Final[Decimal] = Decimal("30000")
DEFAULT_SURCHARGE_PRICE_MAX: Final[Decimal] = Decimal("6000")
MAX_DIMENSION_CM: Final[Decimal] = Decimal("70")
MAX_EFFECTIVE_WEIGHT_KG: Final[Decimal] = Decimal("30")
VOLUMETRIC_WEIGHT_DIVISOR: Final[Decimal] = Decimal("4000")

SURCHARGE_LIGHT: Final[Decimal] = Decimal("90")
SURCHARGE_MEDIUM: Final[Decimal] = Decimal("135")
SURCHARGE_HEAVY: Final[Decimal] = Decimal("200")
SURCHARGE_DEFAULT: Final[Decimal] = Decimal("100")

_LIGHT_WEIGHT_LIMIT: Final[Decimal] = Decimal("2")
_MEDIUM_WEIGHT_LIMIT: Final[Decimal] = Decimal("10")


# Назви та одиниці з фіду постачальника.  Зіставлення назв виконується після
# casefold(), тому варіанти регістру не створюють окремих правил.
_WEIGHT_NAMES: Final[frozenset[str]] = frozenset({"вес", "масса", "вага", "маса"})
_PACKAGE_DIMENSION_PREFIXES: Final[tuple[str, ...]] = (
    "розмір упаковки",
    "размер упаковки",
)
_DIMENSION_PREFIXES: Final[tuple[str, ...]] = ("розміри", "размеры")
_DIMENSION_AXES: Final[dict[str, tuple[str, ...]]] = {
    "height": ("висота", "высота"),
    "length": ("довжина", "длина"),
    "width": ("ширина",),
}

_PARAM_RE: Final[re.Pattern[str]] = re.compile(
    r"<param\b(?P<attributes>[^>]*)>(?P<value>.*?)</param>",
    flags=re.DOTALL | re.IGNORECASE,
)
_ATTRIBUTE_RE: Final[re.Pattern[str]] = re.compile(
    r"(?P<key>[\w:-]+)\s*=\s*(?:\"(?P<double>[^\"]*)\"|'(?P<single>[^']*)')",
)
_NUMBER_RE: Final[re.Pattern[str]] = re.compile(r"(?<![\d.,])(\d+(?:[.,]\d+)?)")
_VALUE_UNIT_RE: Final[re.Pattern[str]] = re.compile(
    r"(?:кг|kg|г|gr?|гр|мм|mm|см|cm|м|m)\b",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class _ProductParam:
    """Нормалізоване представлення одного XML-тегу ``param``."""

    name: str
    unit: str
    value: str


@dataclass(frozen=True)
class _Dimensions:
    """Результат пошуку габаритів у порядку їх пріоритету у фіді."""

    values_cm: tuple[Decimal, Decimal, Decimal] | None
    has_dimension_data: bool
    exceeds_limit: bool


@dataclass(frozen=True)
class _SurchargeDecision:
    """Сума надбавки та ознака fallback-правила без ваги й габаритів."""

    amount: Decimal | None
    is_default: bool = False


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
# Вага, габарити та надбавка за доставку
# ---------------------------------------------------------------------------

def _normalise_label(value: str) -> str:
    """Prepare a parameter name/unit for deterministic case-insensitive matching."""
    return re.sub(r"\s+", " ", unescape(value).strip().casefold())


def _extract_product_params(offer_body: str) -> tuple[_ProductParam, ...]:
    """Extract only raw product parameters; malformed attributes are ignored safely."""
    params: list[_ProductParam] = []
    for match in _PARAM_RE.finditer(offer_body):
        attributes: dict[str, str] = {}
        for attribute in _ATTRIBUTE_RE.finditer(match.group("attributes")):
            value = attribute.group("double") or attribute.group("single") or ""
            attributes[attribute.group("key").casefold()] = unescape(value).strip()

        name = attributes.get("name", "")
        if not name:
            continue
        params.append(
            _ProductParam(
                name=name,
                unit=attributes.get("unit", ""),
                value=unescape(match.group("value")).strip(),
            )
        )
    return tuple(params)


def _parse_numbers(value: str) -> tuple[Decimal, ...]:
    """Parse decimal numbers without converting malformed product text to zero."""
    numbers: list[Decimal] = []
    for raw_number in _NUMBER_RE.findall(value):
        try:
            numbers.append(Decimal(raw_number.replace(",", ".")))
        except InvalidOperation:
            continue
    return tuple(numbers)


def _normalise_unit(unit: str) -> str:
    return re.sub(r"[.\s]", "", _normalise_label(unit))


def _find_unit_in_text(value: str) -> str:
    match = _VALUE_UNIT_RE.search(value)
    return match.group(0) if match else ""


def _unit_factor(unit: str, factors: dict[str, Decimal]) -> Decimal | None:
    return factors.get(_normalise_unit(unit))


_DIMENSION_UNIT_FACTORS: Final[dict[str, Decimal]] = {
    "мм": Decimal("0.1"),
    "mm": Decimal("0.1"),
    "см": Decimal("1"),
    "cm": Decimal("1"),
    "м": Decimal("100"),
    "m": Decimal("100"),
}
_WEIGHT_UNIT_FACTORS: Final[dict[str, Decimal]] = {
    "г": Decimal("0.001"),
    "гр": Decimal("0.001"),
    "gr": Decimal("0.001"),
    "g": Decimal("0.001"),
    "кг": Decimal("1"),
    "kg": Decimal("1"),
}


def _factor_for_param(
    param: _ProductParam,
    factors: dict[str, Decimal],
) -> Decimal | None:
    """Prefer the explicit XML unit, then a unit embedded in the value or name."""
    return (
        _unit_factor(param.unit, factors)
        or _unit_factor(_find_unit_in_text(param.value), factors)
        or _unit_factor(_find_unit_in_text(param.name), factors)
    )


def _is_weight_param(param: _ProductParam) -> bool:
    name = _normalise_label(param.name)
    if "брутто" in name or "brutto" in name:
        return False
    # ``Вес (кг)`` is a duplicate/incorrect source field in the feed and must
    # never be interpreted as a real weight. Only the documented exact names
    # are accepted; the unit comes from ``unit`` or the value itself.
    return name in _WEIGHT_NAMES


def _actual_weight_kg(params: tuple[_ProductParam, ...]) -> Decimal | None:
    """Return the greatest valid actual weight (grams and kilograms are supported)."""
    weights: list[Decimal] = []
    for param in params:
        if not _is_weight_param(param):
            continue
        # Unlike dimension names (for example, ``Ширина, мм``), a weight unit
        # embedded in a parameter name is not reliable. In particular,
        # ``Вес (кг)`` with ``unit=\"\"`` and value ``5365`` must be ignored.
        factor = (
            _unit_factor(param.unit, _WEIGHT_UNIT_FACTORS)
            or _unit_factor(_find_unit_in_text(param.value), _WEIGHT_UNIT_FACTORS)
        )
        numbers = _parse_numbers(param.value)
        if factor is None or not numbers:
            continue
        weight = numbers[0] * factor
        if weight >= 0:
            weights.append(weight)
    return max(weights, default=None)


def _is_package_dimensions_param(param: _ProductParam) -> bool:
    return _normalise_label(param.name).startswith(_PACKAGE_DIMENSION_PREFIXES)


def _is_dimensions_param(param: _ProductParam) -> bool:
    name = _normalise_label(param.name)
    return name.startswith(_DIMENSION_PREFIXES)


def _parse_composite_dimensions(param: _ProductParam) -> tuple[Decimal, Decimal, Decimal] | None:
    """Parse ``L × W × H`` or ``ØD × H`` dimensions from one parameter."""
    factor = _factor_for_param(param, _DIMENSION_UNIT_FACTORS)
    numbers = _parse_numbers(param.value)
    if factor is None:
        return None

    has_diameter = "ø" in param.value.casefold() or "⌀" in param.value
    if has_diameter and len(numbers) >= 2:
        dimensions = (numbers[0], numbers[0], numbers[1])
    elif len(numbers) >= 3:
        dimensions = (numbers[0], numbers[1], numbers[2])
    else:
        return None

    converted = (
        dimensions[0] * factor,
        dimensions[1] * factor,
        dimensions[2] * factor,
    )
    if any(value <= 0 for value in converted):
        return None
    return converted


def _find_composite_dimensions(
    params: tuple[_ProductParam, ...],
    matcher: Callable[[_ProductParam], bool],
) -> _Dimensions:
    """Find the first valid dimension tuple for one priority class of parameters."""
    saw_candidate = False
    for param in params:
        if not matcher(param):
            continue
        saw_candidate = True
        values = _parse_composite_dimensions(param)
        if values is None:
            continue
        return _Dimensions(
            values_cm=values,
            has_dimension_data=True,
            exceeds_limit=max(values) > MAX_DIMENSION_CM,
        )
    return _Dimensions(values_cm=None, has_dimension_data=saw_candidate, exceeds_limit=False)


def _dimension_axis(param: _ProductParam) -> str | None:
    name = _normalise_label(param.name)
    for axis, aliases in _DIMENSION_AXES.items():
        if any(name.startswith(alias) for alias in aliases):
            return axis
    return None


def _parse_single_dimension(param: _ProductParam) -> Decimal | None:
    factor = _factor_for_param(param, _DIMENSION_UNIT_FACTORS)
    numbers = _parse_numbers(param.value)
    if factor is None or not numbers:
        return None
    value = numbers[0] * factor
    return value if value > 0 else None


def _find_separate_dimensions(params: tuple[_ProductParam, ...]) -> _Dimensions:
    values: dict[str, Decimal] = {}
    saw_candidate = False
    for param in params:
        axis = _dimension_axis(param)
        if axis is None:
            continue
        saw_candidate = True
        value = _parse_single_dimension(param)
        if value is not None and axis not in values:
            values[axis] = value

    if len(values) != len(_DIMENSION_AXES):
        return _Dimensions(
            values_cm=None,
            has_dimension_data=saw_candidate,
            exceeds_limit=any(value > MAX_DIMENSION_CM for value in values.values()),
        )

    dimensions = (values["length"], values["width"], values["height"])
    return _Dimensions(
        values_cm=dimensions,
        has_dimension_data=True,
        exceeds_limit=max(dimensions) > MAX_DIMENSION_CM,
    )


def _dimensions_cm(params: tuple[_ProductParam, ...]) -> _Dimensions:
    """Use shipping package dimensions, then general dimensions, then separate sides."""
    package_dimensions = _find_composite_dimensions(params, _is_package_dimensions_param)
    if package_dimensions.has_dimension_data:
        return package_dimensions

    general_dimensions = _find_composite_dimensions(params, _is_dimensions_param)
    if general_dimensions.has_dimension_data:
        return general_dimensions

    return _find_separate_dimensions(params)


def _surcharge_for_weight(weight: Decimal) -> _SurchargeDecision:
    """Map the greatest actual/volumetric weight to one delivery surcharge."""
    if weight > MAX_EFFECTIVE_WEIGHT_KG:
        return _SurchargeDecision(amount=None)
    if weight < _LIGHT_WEIGHT_LIMIT:
        return _SurchargeDecision(amount=SURCHARGE_LIGHT)
    if weight < _MEDIUM_WEIGHT_LIMIT:
        return _SurchargeDecision(amount=SURCHARGE_MEDIUM)
    return _SurchargeDecision(amount=SURCHARGE_HEAVY)


def _surcharge_for_offer(price: Decimal, offer_body: str) -> _SurchargeDecision:
    """Select a delivery surcharge after the channel price has been calculated."""
    if not SURCHARGE_PRICE_MIN <= price <= SURCHARGE_PRICE_MAX:
        return _SurchargeDecision(amount=None)

    params = _extract_product_params(offer_body)
    actual_weight = _actual_weight_kg(params)
    dimensions = _dimensions_cm(params)

    # A known side above 70 cm disqualifies the offer even when the other sides
    # are absent.  Shipping-package dimensions take precedence over product ones.
    if dimensions.exceeds_limit:
        return _SurchargeDecision(amount=None)

    if dimensions.values_cm is None:
        # A valid actual weight is sufficient when volumetric weight cannot be
        # calculated. A known side over 70 cm was rejected above.
        if actual_weight is not None:
            return _surcharge_for_weight(actual_weight)

        # With neither weight nor all three dimensions, use the +100 fallback.
        # A partial small dimension (for example, only length=90 mm) does not
        # prevent this fallback.
        if price <= DEFAULT_SURCHARGE_PRICE_MAX:
            return _SurchargeDecision(amount=SURCHARGE_DEFAULT, is_default=True)
        return _SurchargeDecision(amount=None)

    volumetric_weight = (
        dimensions.values_cm[0] * dimensions.values_cm[1] * dimensions.values_cm[2]
    ) / VOLUMETRIC_WEIGHT_DIVISOR
    effective_weight = max(
        value for value in (actual_weight, volumetric_weight) if value is not None
    )
    return _surcharge_for_weight(effective_weight)


# ---------------------------------------------------------------------------
# Логер
# ---------------------------------------------------------------------------

def _build_logger() -> logging.Logger:
    """
    Logger that writes to logs/epicenter_default_id.log.
    Overwrites on every run (mode='w') so the log always matches the current feed.
    """
    logger = logging.getLogger("epicenter.default_offers")
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    DEFAULT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(DEFAULT_LOG_PATH, mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


def _write_default_markup_offer_ids(offer_ids: list[str]) -> None:
    """Overwrite the fallback-markup ID log so it never contains stale offers."""
    ids = ", ".join(offer_ids) if offer_ids else "—"
    try:
        DEFAULT_MARKUP_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        DEFAULT_MARKUP_LOG_PATH.write_text(
            f"Offer IDs з націнкою за замовчуванням: {ids}\n",
            encoding="utf-8",
        )
    except OSError as exc:
        print(
            "⚠️  Epicenter: не вдалося записати ID товарів з націнкою за "
            f"замовчуванням у {DEFAULT_MARKUP_LOG_PATH.name}: {exc}"
        )


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
    surcharge_counts: dict[Decimal, int] = {
        SURCHARGE_LIGHT: 0,
        SURCHARGE_DEFAULT: 0,
        SURCHARGE_MEDIUM: 0,
        SURCHARGE_HEAVY: 0,
    }
    default_surcharge_offer_ids: list[str] = []

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

                # Крок 5: надбавка лише після повного розрахунку за
                # коефіцієнтами CSV та округлення. Вагу й габарити читаємо з
                # початкового тіла офера, тому інші XML-трансформації не
                # впливають на детермінований вибір тарифу.
                surcharge = _surcharge_for_offer(new_price, body)
                if surcharge.amount is not None:
                    new_price += surcharge.amount
                    surcharge_counts[surcharge.amount] += 1
                    if surcharge.is_default:
                        default_surcharge_offer_ids.append(offer_id)

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

    print("Epicenter surcharges:")
    for amount, label in (
        (SURCHARGE_LIGHT, ""),
        (SURCHARGE_DEFAULT, " (за замовчуванням)"),
        (SURCHARGE_MEDIUM, ""),
        (SURCHARGE_HEAVY, ""),
    ):
        print(f"{surcharge_counts[amount]} товарів з націнкою {amount} грн{label}")
    _write_default_markup_offer_ids(default_surcharge_offer_ids)

    if stats.price_exceptions:
        print(f"⚠️  Epicenter: {stats.price_exceptions} оферів з винятком при розрахунку ціни (ціна з вхідного XML залишена без змін) — деталі: {DEFAULT_LOG_PATH.name}")

    errors: list[str] = []

    if stats.no_category_rules:
        # Prom автоматично перемістив товари у нові категорії без правил.
        # Фід згенеровано з coef_uncategorized — ціни некоректні.
        # Потрібно додати правила у epicenter_coefficients.csv.
        # Деталі у logs/epicenter_default_id.log
        ids_str = ", ".join(no_rule_offer_ids)
        errors.append(
            f"{stats.no_category_rules} товарів без правил категорії (no_category_rules). "
            f"Додайте правила у epicenter_coefficients.csv. Деталі: logs/epicenter_default_id.log\n"
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
