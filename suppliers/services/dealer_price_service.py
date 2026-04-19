"""
Загальний сервіс ціноутворення для дилерських пауків.

Відповідальність: ТІЛЬКИ обчислення цін.
- Конвертація dealer USD → UAH (для viatec)
- Вибір ціни для каналу prom (retail vs dealer залежно від порогу)
- Вибір ціни для каналу site (retail vs dealer залежно від порогу)

Не знає нічого про Scrapy, CSV, пайплайн.

Пороги по постачальниках:
  VIATEC_PROM_THRESHOLD = 1.35
  VIATEC_SITE_THRESHOLD = 1.3
  SECUR_PROM_THRESHOLD  = 1.3
  SECUR_SITE_THRESHOLD  = 1.3
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP, InvalidOperation


# ── Пороги ────────────────────────────────────────────────────────────────────

VIATEC_PROM_THRESHOLD: Decimal = Decimal("1.35")
"""retail / dealer >= 1.35 → prom отримує роздрібну × coef_retail (viatec)."""

VIATEC_SITE_THRESHOLD: Decimal = Decimal("1.3")
"""retail / dealer >= 1.3 → site отримує роздрібну × coef_retail (viatec)."""

SECUR_PROM_THRESHOLD: Decimal = Decimal("1.3")
"""retail / dealer >= 1.30 → prom отримує роздрібну × coef_retail (secur)."""

SECUR_SITE_THRESHOLD: Decimal = Decimal("1.3")
"""retail / dealer >= 1.3 → site отримує роздрібну × coef_retail (secur)."""


# ── Дефолти ───────────────────────────────────────────────────────────────────

DEFAULT_USD_RATE: Decimal = Decimal("43.8")
"""Курс USD за замовчуванням — якщо парсинг не вдався (viatec)."""

DEFAULT_COEF_RETAIL: Decimal = Decimal("1")
DEFAULT_COEF_DEALER: Decimal = Decimal("1.2")


# ── Сервіс ────────────────────────────────────────────────────────────────────

class DealerPriceService:
    """
    Обчислення цін для каналів prom / site по дилерській ціні.

    Всі методи — staticmethod (відсутній стан, детермінізм).
    threshold передається явно — кожен постачальник і канал мають свій поріг.
    """

    # ------------------------------------------------------------------ #
    # КОНВЕРТАЦІЯ / ПРИВЕДЕННЯ ТИПІВ
    # ------------------------------------------------------------------ #

    @staticmethod
    def to_decimal(value: str | Decimal | float | int, fallback: Decimal) -> Decimal:
        """Безпечне приведення до Decimal; повертає fallback при помилці."""
        if isinstance(value, Decimal):
            return value
        try:
            clean = str(value).strip().replace(",", ".").replace(" ", "")
            if not clean:
                return fallback
            return Decimal(clean)
        except InvalidOperation:
            return fallback

    @staticmethod
    def dealer_uah(dealer_usd: str | Decimal, usd_rate: str | Decimal) -> Decimal:
        """
        Конвертує дилерську ціну USD → UAH (viatec).

        dealer_uah = dealer_usd × usd_rate
        Повертає 0 якщо вхідні дані некоректні.
        """
        price = DealerPriceService.to_decimal(dealer_usd, Decimal("0"))
        rate  = DealerPriceService.to_decimal(usd_rate, DEFAULT_USD_RATE)
        if rate <= 0:
            rate = DEFAULT_USD_RATE
        return price * rate

    # ------------------------------------------------------------------ #
    # ЦІНИ ДЛЯ КАНАЛІВ
    # ------------------------------------------------------------------ #

    @staticmethod
    def _channel_price(
        retail_uah: str | Decimal,
        dealer_uah_val: Decimal,
        coef_retail: Decimal,
        coef_dealer: Decimal,
        threshold: Decimal,
    ) -> Decimal:
        """
        Внутрішня логіка вибору ціни за порогом (спільна для prom і site).

        retail / dealer >= threshold → retail × coef_retail
        retail / dealer <  threshold → dealer × coef_dealer
        Fallback (retail = 0 або dealer = 0) → dealer × coef_dealer
        """
        retail = DealerPriceService.to_decimal(retail_uah, Decimal("0"))

        if retail > 0 and dealer_uah_val > 0:
            if retail / dealer_uah_val >= threshold:
                return retail * coef_retail

        return dealer_uah_val * coef_dealer

    @staticmethod
    def prom_price(
        retail_uah: str | Decimal,
        dealer_uah_val: Decimal,
        coef_retail: Decimal,
        coef_dealer: Decimal,
        threshold: Decimal = VIATEC_PROM_THRESHOLD,
    ) -> Decimal:
        """
        Ціна для каналу prom.

        retail / dealer >= threshold → retail × coef_retail
        retail / dealer <  threshold → dealer × coef_dealer
        """
        return DealerPriceService._channel_price(
            retail_uah, dealer_uah_val, coef_retail, coef_dealer, threshold
        )

    @staticmethod
    def site_price(
        retail_uah: str | Decimal,
        dealer_uah_val: Decimal,
        coef_retail: Decimal,
        coef_dealer: Decimal,
        threshold: Decimal = VIATEC_SITE_THRESHOLD,
    ) -> Decimal:
        """
        Ціна для каналу site.

        retail / dealer >= threshold → retail × coef_retail
        retail / dealer <  threshold → dealer × coef_dealer
        """
        return DealerPriceService._channel_price(
            retail_uah, dealer_uah_val, coef_retail, coef_dealer, threshold
        )

    # ------------------------------------------------------------------ #
    # ФОРМАТУВАННЯ
    # ------------------------------------------------------------------ #

    @staticmethod
    def format_price(price: Decimal, decimal_places: int = 0) -> str:
        """
        Форматує Decimal → str для CSV.

        decimal_places=0 → ціле число (округлення ROUND_HALF_UP).
        """
        if price <= 0:
            return ""
        if decimal_places == 0:
            return str(int(price.quantize(Decimal("1"), rounding=ROUND_HALF_UP)))
        fmt = "0." + "0" * decimal_places
        return str(price.quantize(Decimal(fmt), rounding=ROUND_HALF_UP))

    # ------------------------------------------------------------------ #
    # ПАРСИНГ КУРСУ З HTML (viatec-specific)
    # ------------------------------------------------------------------ #

    @staticmethod
    def parse_usd_rate_from_response(response) -> Decimal | None:
        """
        Витягує поточний USD б/г курс із навігації viatec.ua.

        HTML структура (2 параграфи: USD і USD б/г):
            <p class="lk-nav__admin-bottom-dollar-usd ...">
                <span class="lk-nav__admin-bottom-dollar-usd-name">USD</span>
                <span class="lk-nav__admin-bottom-dollar-usd-value ...">43.90</span>
            </p>
            <p class="lk-nav__admin-bottom-dollar-usd ...">
                <span class="lk-nav__admin-bottom-dollar-usd-name">USD б/г</span>
                <span class="lk-nav__admin-bottom-dollar-usd-value ...">44.00</span>
            </p>

        CSS .get() повертає перший збіг (43.90) — потрібен другий.
        XPath вибирає <p> що містить текст 'б/г' і читає валюту з нього.

        Повертає Decimal або None якщо тег не знайдено / некоректне значення.
        """
        raw = response.xpath(
            "//p[contains(@class,'lk-nav__admin-bottom-dollar-usd')]"
            "[.//span[contains(@class,'lk-nav__admin-bottom-dollar-usd-name')"
            "         and contains(text(),'б/г')]]"
            "//span[contains(@class,'lk-nav__admin-bottom-dollar-usd-value')]/text()"
        ).get()
        if not raw:
            return None
        rate = DealerPriceService.to_decimal(raw.strip(), Decimal("0"))
        return rate if rate > 0 else None
