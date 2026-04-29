"""
Spider для парсингу дилерських цін з viatec.ua — фід-driven режим.

ВІДМІНА ВІД dealer.py:
  - Немає category crawling, пагінації, _load_category_mapping
  - Старт: логін → after_login ітерує по feed_service.get_all_urls()
  - UA-дані (name_ua, description_ua, image, params) — з фіду, не зі сторінки UA
  - parse_product_ru — єдиний парсер сторінки (RU), мерджить з ua-мета

ПОТІК:
  1. ViatecFeedFullService при старті:
       завантажує фід → будує:
         {url: FeedProduct(name_ua, description_ua, image, available, params)}
         {sku: vendor}

  2. start() → логін

  3. after_login() → ітерація по feed_service.get_all_urls()
       → yield Request(ru_url, callback=parse_product_ru)
       з meta {name_ua, description_ua, image, params} з фіду

  4. parse_product_ru() — забирає зі сторінки:
       name_ru, description_ru, dealer_price, rrp, gallery, availability, quantity, specs
       + мерджить з ua-даними з meta
"""

from __future__ import annotations

import csv
import os
from decimal import Decimal
from pathlib import Path

import scrapy
from dotenv import load_dotenv

from suppliers.services.dealer_price_service import (
    DEFAULT_USD_RATE,
    DealerPriceService as ViatecPriceService,
)
from suppliers.services.viatec_feed_full_service import ViatecFeedFullService
from suppliers.spiders.base import BaseDealerSpider, ViatecBaseSpider

_PRIORITY_PRODUCT = 10


class ViatecFeedFullSpider(ViatecBaseSpider, BaseDealerSpider):
    """
    Дилерський паук viatec.ua на базі XML-фіду.

    Запуск:
        python scripts/ultra_clean_run.py viatec_feed_full
    """

    name = "viatec_feed_full"
    supplier_id = "viatec"
    output_filename = "viatec_new.csv"

    custom_settings = {
        **ViatecBaseSpider.custom_settings,
        "COOKIES_ENABLED": True,
        "HTTPERROR_ALLOWED_CODES": [404, 500, 502, 503],
        "ITEM_PIPELINES": {
            "suppliers.pipelines.SuppliersPipeline": 300,
        },
    }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        import logging
        logging.getLogger("scrapy.crawler").setLevel(logging.WARNING)

        _project_root = Path(
            os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline")
        )
        load_dotenv(_project_root / "suppliers" / ".env")
        self.email: str = os.getenv("VIATEC_EMAIL", "")
        self.password: str = os.getenv("VIATEC_PASSWORD", "")

        if not self.email or not self.password:
            raise ValueError(
                "❌ Відсутні VIATEC_EMAIL / VIATEC_PASSWORD. "
                "Локально: додайте в suppliers/.env. CI: додайте в GitHub Secrets."
            )

        self.usd_rate: Decimal = DEFAULT_USD_RATE
        self._project_root = _project_root

        # ── XML-фід: ua-дані + виробники ──────────────────────────────────
        try:
            self.feed_service = ViatecFeedFullService(logger=self.logger)
        except Exception as exc:
            raise RuntimeError(
                f"❌ ViatecFeedFullService не ініціалізовано: {exc}"
            ) from exc

        if not self.feed_service.loaded:
            raise RuntimeError(
                "❌ ViatecFeedFullService: фід порожній або недоступний."
            )

        # ── RESUME: завантажуємо вже спарсені URL з попереднього запуску ──
        already_scraped = self._load_already_scraped_urls()
        self.processed_products.update(already_scraped)

    # ------------------------------------------------------------------ #
    # RESUME
    # ------------------------------------------------------------------ #

    def _load_already_scraped_urls(self) -> set[str]:
        """
        Читає вже збережений output CSV і повертає set UA-URL.
        При першому запуску файл не існує → порожній set.
        """
        urls: set[str] = set()
        out_path = self._project_root / "data" / "output" / self.output_filename
        if not out_path.exists():
            return urls
        try:
            with open(out_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    url = row.get("Продукт_на_сайті", "").strip()
                    if url:
                        urls.add(url)
            self.logger.info(
                f"📋 Resume: знайдено {len(urls)} вже спарсених товарів — пропускаємо"
            )
        except Exception as exc:
            self.logger.warning(f"⚠️  Не вдалося завантажити resume CSV: {exc}")
        return urls

    # ------------------------------------------------------------------ #
    # AUTH
    # ------------------------------------------------------------------ #

    async def start(self):
        yield scrapy.Request(
            "https://viatec.ua/login",
            callback=self.parse_login_page,
            dont_filter=True,
        )

    def parse_login_page(self, response):
        csrf = response.css("input[name=_token]::attr(value)").get()
        if not csrf:
            self.logger.error("Не знайдено CSRF (_token) на сторінці логіну!")
            return
        self.logger.info(f"Знайдено CSRF: {csrf}")
        yield scrapy.FormRequest(
            url="https://viatec.ua/login",
            method="POST",
            formdata={
                "_token": csrf,
                "email": self.email,
                "password": self.password,
            },
            callback=self.after_login,
            dont_filter=True,
        )

    def after_login(self, response):
        if b"viatec_session" not in b" ".join(
            response.headers.getlist("Set-Cookie")
        ):
            self.logger.error("Авторизація не виконана!")
            return

        self.logger.info("✅ УСПІШНИЙ ЛОГІН")

        # USD курс береться з фіду (вже розпарсений при старті сервісу)
        if self.feed_service.usd_rate is not None:
            self.usd_rate = self.feed_service.usd_rate
            self.logger.info(f"💱 USD курс з фіду: {self.usd_rate}")
        else:
            self.logger.warning(f"⚠️ USD курс не знайдено у фіді, використовується дефолт: {self.usd_rate}")

        total = len(self.feed_service)
        self.logger.info(f"📦 Починаємо обробку {total} товарів з фіду")

        for ua_url in self.feed_service.get_all_urls():
            if ua_url in self.processed_products:
                continue
            self.processed_products.add(ua_url)

            product = self.feed_service.get_product_data(ua_url)
            if product is None:
                continue

            ru_url = self._convert_to_ru_url(ua_url)
            yield scrapy.Request(
                url=ru_url,
                callback=self.parse_product_ru,
                errback=self.parse_product_error,
                meta={
                    "original_url": ua_url,
                    "name_ua": product.name_ua,
                    "description_ua": product.description_ua,
                    "image_feed": product.image,
                    "feed_params": product.params,
                },
                priority=_PRIORITY_PRODUCT,
                dont_filter=True,
            )

    # ------------------------------------------------------------------ #
    # USD RATE
    # ------------------------------------------------------------------ #

    def _try_update_usd_rate(self, response, source: str = "") -> None:
        rate = ViatecPriceService.parse_usd_rate_from_response(response)
        if rate is not None and rate != self.usd_rate:
            self.usd_rate = rate
            self.logger.info(f"💱 USD б/г курс: {self.usd_rate} ({source})")

    # ------------------------------------------------------------------ #
    # PARSE PRODUCT RU
    # ------------------------------------------------------------------ #

    def parse_product_ru(self, response):
        try:
            # ── RU-дані зі сторінки ──────────────────────────────────────
            name_ru = (response.css("h1::text").get() or "").strip()
            description_ru = self._extract_description_with_br(response)

            supplier_sku = (
                response.css(
                    "span.card-header__card-articul-text-value::text"
                ).get()
                or ""
            ).strip()

            price_raw = (
                response.css("div.card-header__card-price-new::text").get() or ""
            ).strip().replace("\xa0", "").replace(" ", "")
            price = self._clean_price(price_raw) if price_raw else ""

            price_rrp_uah = self._parse_rrp_uah(response)

            # Галерея зі сторінки (повний набір, фід дає тільки одне фото)
            gallery_images = response.css(
                'a[data-fancybox*="gallery"]::attr(href)'
            ).getall()
            if not gallery_images:
                gallery_images = response.css(
                    "img.card-header__card-images-image::attr(src)"
                ).getall()

            image_urls = [
                s
                for img in gallery_images
                if (s := self._sanitize_image_url(response.urljoin(img)))
            ]
            # Фалбек: фото з фіду якщо галерея порожня
            if not image_urls:
                feed_img = response.meta.get("image_feed", "")
                if feed_img:
                    image_urls = [self._sanitize_image_url(feed_img)]

            image_url = ", ".join(image_urls)

            availability_raw = response.css(
                "div.card-header__card-status-badge::text"
            ).get()
            availability = self._normalize_availability(availability_raw)
            quantity = self._extract_quantity(availability_raw)

            specs_list = self._extract_specifications(response)

            # ── UA-дані з meta (фід) ──────────────────────────────────────
            name_ua = response.meta.get("name_ua", "")
            description_ua = response.meta.get("description_ua", "")
            original_url = response.meta.get("original_url", response.url)

            item = {
                "Код_товару": "",
                "Назва_позиції": name_ru,
                "Назва_позиції_укр": name_ua,
                "Пошукові_запити": "",
                "Пошукові_запити_укр": "",
                "Опис": description_ru,
                "Опис_укр": description_ua,
                "Тип_товару": "r",
                "Ціна": price,
                "Валюта": self.currency,
                "Одиниця_виміру": "шт.",
                "Посилання_зображення": image_url,
                "Наявність": availability,
                "Кількість": quantity,
                "Назва_групи": "",
                "Назва_групи_укр": "",
                "Номер_групи": "",
                "Ідентифікатор_товару": supplier_sku,
                "Ідентифікатор_підрозділу": "",
                "Посилання_підрозділу": "",
                "Виробник": self.feed_service.get_vendor(supplier_sku),
                "Країна_виробник": "",
                "price_rrp_uah": price_rrp_uah,
                "price_type": self.price_type,
                "supplier_id": self.supplier_id,
                "usd_rate": str(self.usd_rate),
                "output_file": self.output_filename,
                "Продукт_на_сайті": original_url,
                "category_url": "",
                "specifications_list": specs_list,
            }
            yield item

        except Exception as exc:
            self.logger.error(
                f"❌ Помилка парсингу (RU): {response.url} | {exc}"
            )

    # ------------------------------------------------------------------ #
    # HELPERS (перенесені з dealer.py)
    # ------------------------------------------------------------------ #

    def _parse_rrp_uah(self, response) -> str:
        """
        Парсить ціну РРЦ в гривнях зі сторінки товару.

        Шукає:  <p class="font-0-9 color-gray-80 mb-1">2 099.00 грн (РРЦ)</p>

        Повертає очищену числову рядок або "".
        """
        for para in response.css("p.font-0-9.color-gray-80.mb-1"):
            raw = "".join(para.css("::text").getall())
            if "РРЦ" not in raw:
                continue
            cleaned = raw.replace("\xa0", "").replace(" ", "").strip()
            price = self._clean_price(cleaned) if cleaned else ""
            if price:
                return price
        return ""

    def parse_product_error(self, failure):
        url = failure.request.url
        name = failure.request.meta.get("name_ua", "Назва не знайдена")
        self.logger.error(f"❌ Помилка товару: {name} ({url}). {failure.value}")
        self.failed_products.append(
            {"url": url, "reason": str(failure.value), "product_name": name}
        )
