"""
Загальний сервіс ціноутворення для дилерських пауків.

Відповідальність: ТІЛЬКИ обчислення цін.

Формула (канали site і prom):
    X    = retail / dealer * coef
    Ціна = dealer * X          якщо X > threshold  (= retail * coef)
    Ціна = dealer * threshold  якщо X <= threshold

    Еквівалентно: Ціна = max(retail * coef, dealer * threshold)

Параметри coef і threshold — у category.csv (рядок каналу).
Жодних hardcoded порогів у сервісі.

Особливий випадок:
    retail < dealer (помилка постачальника) → swap + warning у лог.
"""
from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP, InvalidOperation


# ── Дефолти ───────────────────────────────────────────────────────────────────

DEFAULT_USD_RATE: Decimal = Decimal("44.5")
"""Курс USD за замовчуванням — якщо парсинг курсу не вдався (viatec)."""


# ── Сервіс ────────────────────────────────────────────────────────────────────

class DealerPriceService:
    """
    Обчислення цін для каналів prom / site по дилерській ціні.

    Всі методи — staticmethod (відсутній стан, детермінізм).
    coef і threshold передаються явно з ChannelConfig (завантажені з CSV).
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
    # ЦІНА КАНАЛУ
    # ------------------------------------------------------------------ #

    @staticmethod
    def channel_price(
        retail_uah: str | Decimal,
        dealer_uah_val: Decimal,
        coef: Decimal,
        threshold: Decimal,
        *,
        logger=None,
        product_name: str = "",
    ) -> Decimal:
        """
        Обчислює ціну для каналу (prom або site) за формулою:

            X    = retail / dealer * coef
            Ціна = dealer * X          якщо X > threshold  (= retail * coef)
            Ціна = dealer * threshold  якщо X <= threshold

        Еквівалентно: Ціна = max(retail * coef, dealer * threshold)

        Особливий випадок (помилка постачальника):
            retail < dealer → swap(retail, dealer) + warning у лог.

        Fallback:
            retail = 0 або dealer = 0 → Ціна = dealer * threshold
        """
        retail = DealerPriceService.to_decimal(retail_uah, Decimal("0"))
        dealer = dealer_uah_val

        if retail <= 0 or dealer <= 0:
            return dealer * threshold

        # Swap if supplier gave dealer > retail (error in their feed)
        if retail < dealer:
            if logger:
                logger.warning(
                    f"⚠️ retail < dealer — постачальник переплутав ціни: "
                    f"retail={retail}, dealer={dealer} | {product_name}"
                )
            retail, dealer = dealer, retail

        return max(retail * coef, dealer * threshold)

    @staticmethod
    def channel_price_for_config(
        channel_config,
        retail_uah: str | Decimal,
        dealer_uah_val: Decimal,
        *,
        logger=None,
        product_name: str = "",
    ) -> Decimal:
        """
        Обчислює ціну для рядка каналу за ChannelConfig-подібним об'єктом.

        coef і threshold беруться з channel_config (завантажені з CSV).
        Не знає нічого про Scrapy чи pipeline — немає циклічних імпортів.
        """
        return DealerPriceService.channel_price(
            retail_uah=retail_uah,
            dealer_uah_val=dealer_uah_val,
            coef=channel_config.coef,
            threshold=channel_config.threshold,
            logger=logger,
            product_name=product_name,
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
