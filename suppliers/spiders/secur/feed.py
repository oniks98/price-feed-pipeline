"""
Spider: Feed-only (Ajax + Імпорт) — без Playwright

ЛОГІКА:
  1. UA фід (XML) → кешує: назви, описи, <param> хар-ки (УКРАЇНСЬКОЮ)
  2. RU фід (XML) → збирає ціни, yield item напряму в pipeline
  3. CategorySpecsEnricher → доповнення категорійними хар-ками з CSV

ЧОМУ БЕЗ PLAYWRIGHT:
  Cloudflare блокує сторінки secur.ua з IP GitHub Actions (HTTP 403).
  Нові токенізовані фіди на export.secur.ua доступні без обмежень.

ФІДИ (логічні ID → реальні URL):
  50 (Ajax):   https://export.secur.ua/feed/export/v2/8f60b225-...
  52 (Імпорт): https://export.secur.ua/feed/export/v2/9f69564b-...

  CSV-колонка `feed` залишається "50" / "52" — логічні ідентифікатори.
  Реальні URL зберігаються у FEED_URL_MAP → CSV оновлювати не потрібно.

ПОТІК ЗАПИТІВ:
  start()
    └─ parse_ua_feed()   [XML — кешує назви/описи/<param>]
         └─ parse_ru_feed()   [XML — yield item напряму]
              └─ (next feed_index) ...

ЦІНИ (_resolve_price):
  retail  = <price>       (РРЦ / роздрібна)
  dealer  = <dealerPrice> (дилерська)
  dealer > retail → WARNING, dealer = retail (захист від брудних даних)
  dealerPrice відсутня  → dealer = retail (fallback)

ХАРАКТЕРИСТИКИ:
  Тільки з <param> UA-фіду (УКРАЇНСЬКА мова).
  Доповнюємо CategorySpecsEnricher.
  Сторінки не відкриваємо — Cloudflare блокує.

ЗОБРАЖЕННЯ:
  Тільки з фіду (усі <picture> теги → ", ".join).

ДЕДУПЛІКАЦІЯ:
  processed_seen: set[tuple[feed_id, product_url]]
  Однаковий URL в різних фідах НЕ є дублем.

RESUME:
  При старті читає вже збережений output CSV → пропускає вже оброблені URL.
"""

import csv
import logging
import os
import re
from collections.abc import AsyncGenerator, Iterator
from pathlib import Path

import scrapy
from scrapy.http import Response
from scrapy.selector import Selector

from suppliers.services.category_specs_enricher import CategorySpecsEnricher


# ─────────────────────────────────────────────────────────────
# Конфіг: логічний feed_id → токенізований URL постачальника
# ─────────────────────────────────────────────────────────────
FEED_URL_MAP: dict[str, str] = {
    "50": "https://export.secur.ua/feed/export/v2/8f60b225-2273-4456-ba5a-297f3f786120",
    "52": "https://export.secur.ua/feed/export/v2/9f69564b-341a-4878-8167-9931d2481bba",
}


class SecurFeedSpider(scrapy.Spider):
    """
    Feed-only паук secur.ua: ціни + хар-ки з XML без Playwright.

    Запуск:
        scrapy crawl secur_feed
    """

    name = "secur_feed"
    supplier_id = "secur"
    allowed_domains = ["export.secur.ua"]

    ALL_FEED_IDS: list[str] = list(FEED_URL_MAP.keys())

    custom_settings = {
        "ITEM_PIPELINES": {
            "suppliers.pipelines.SuppliersPipeline": 300,
        },
        # Нові фіди — чистий HTTP, без Playwright, без Cloudflare.
        # 4 паралельних запити, без штучної затримки.
        # Retry тільки для серверних помилок 5xx (не для 403/404).
        "CONCURRENT_REQUESTS": 4,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "DOWNLOAD_DELAY": 0,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "RETRY_HTTP_CODES": [500, 502, 503, 504, 520, 521, 522, 524],
        "HTTPERROR_ALLOW_ALL": True,
    }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        logging.getLogger("scrapy.crawler").setLevel(logging.WARNING)

        self.output_filename = "secur_new.csv"
        self.currency = "UAH"
        self.price_type = "retail"

        self._root = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))

        self.category_mapping: dict[tuple[str, str], dict] = self._load_category_mapping()

        csv_path = str(self._root / "data" / "secur" / "secur_category.csv")
        self.category_enricher = CategorySpecsEnricher(csv_path, self.supplier_id)

        # Кеш UA-даних поточного фіду: {product_id → {name_ua, description_ua, specs_from_feed}}
        self.products_ua: dict[str, dict] = {}

        # Дедуплікація між запусками та між фідами
        self.processed_seen: set[tuple[str, str]] = set()
        for url in self._load_already_scraped_urls():
            for fid in self.ALL_FEED_IDS:
                self.processed_seen.add((fid, url))

    # ──────────────────────────────────────────────────────────────────
    # RESUME
    # ──────────────────────────────────────────────────────────────────

    def _load_already_scraped_urls(self) -> set[str]:
        """Читає вже збережений output CSV → set URL для пропуску."""
        urls: set[str] = set()
        out_path = self._root / "data" / "output" / self.output_filename
        if not out_path.exists():
            return urls
        try:
            with open(out_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    url = row.get("Продукт_на_сайті", "").strip()
                    if url:
                        urls.add(url)
            self.logger.info(f"📋 Resume: {len(urls)} вже спарсених — пропускаємо")
        except Exception as exc:
            self.logger.error(f"⚠️ Resume load failed: {exc}")
        return urls

    # ──────────────────────────────────────────────────────────────────
    # CATEGORY MAPPING
    # ──────────────────────────────────────────────────────────────────

    def _load_category_mapping(self) -> dict:
        """
        Завантажує secur_category.csv → dict {(feed_id, category_id): info}.
        Логічні feed_id ("50", "52") відповідають колонці `feed` в CSV.
        """
        mapping: dict = {}
        csv_path = self._root / "data" / "secur" / "secur_category.csv"
        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    if row.get("channel", "").strip() != "site":
                        continue
                    category_id = row.get("category id", "").strip()
                    if not category_id:
                        continue
                    feed_id = row.get("feed", "").strip()
                    key = (feed_id, category_id)
                    if key in mapping:
                        continue
                    mapping[key] = {
                        "category_name":    row.get("Назва_групи", ""),
                        "group_number":     row.get("Номер_групи", ""),
                        "subdivision_id":   row.get("Ідентифікатор_підрозділу", "").strip(),
                        "subdivision_link": row.get("Посилання_підрозділу", ""),
                        "category_url":     row.get("Линк категории поставщика", "").strip().strip('"'),
                        "feed":             feed_id,
                    }
            self.logger.info(f"✅ Category mappings: {len(mapping)}")
        except Exception as exc:
            self.logger.error(f"❌ Category mapping load failed: {exc}")
        return mapping

    def _is_deleted_category(self, category_id: str) -> bool:
        return (
            self.category_mapping
            .get(("", category_id), {})
            .get("subdivision_id", "")
            .lower() == "delete"
        )

    def _get_category_info(self, feed_id: str, category_id: str) -> dict:
        return (
            self.category_mapping.get((feed_id, category_id))
            or self.category_mapping.get(("", category_id))
            or {}
        )

    # ──────────────────────────────────────────────────────────────────
    # CRAWLING
    # ──────────────────────────────────────────────────────────────────

    async def start(self) -> AsyncGenerator[scrapy.Request, None]:
        self.logger.info(f"🚀 Починаю обробку фідів: {self.ALL_FEED_IDS}")
        yield self._ua_request(feed_index=0)

    def _ua_request(self, feed_index: int) -> scrapy.Request:
        fid = self.ALL_FEED_IDS[feed_index]
        return scrapy.Request(
            url=FEED_URL_MAP[fid],
            callback=self.parse_ua_feed,
            meta={"feed_id": fid, "feed_index": feed_index},
            dont_filter=True,
        )

    def parse_ua_feed(self, response: Response) -> Iterator[scrapy.Request]:
        """
        UA XML-фід → кешує назви, описи, хар-ки з <param> (УКРАЇНСЬКОЮ).
        Після кешування → запускає RU-фід того ж feed_id.
        """
        feed_id: str = response.meta["feed_id"]
        feed_index: int = response.meta["feed_index"]

        if response.status != 200:
            self.logger.error(
                f"❌ UA фід {feed_id}: HTTP {response.status} — пропускаємо фід"
            )
            return

        selector = Selector(text=response.text, type="xml")
        selector.remove_namespaces()
        self.logger.info(
            f"📂 [{feed_index + 1}/{len(self.ALL_FEED_IDS)}] UA фід {feed_id} — парсинг..."
        )

        self.products_ua = {}
        with_params = 0

        for offer in selector.xpath("//offer"):
            product_id = offer.xpath("@id").get()
            if not product_id:
                continue
            category_id = offer.xpath("categoryId/text()").get() or ""
            if self._is_deleted_category(category_id):
                continue

            name_ua = re.sub(
                r'\s*\([A-Z0-9\.]+\)\s*$',
                "",
                offer.xpath("name/text()").get() or "",
            ).strip()

            specs = self._parse_feed_specs(offer)
            if specs:
                with_params += 1

            self.products_ua[product_id] = {
                "name_ua":         name_ua,
                "description_ua":  self._clean_description(
                    offer.xpath("description/text()").get()
                ),
                "specs_from_feed": specs,
            }

        self.logger.info(
            f"✅ UA {feed_id}: {len(self.products_ua)} у кеші "
            f"({with_params} з <param> хар-ками)"
        )

        yield scrapy.Request(
            url=f"{FEED_URL_MAP[feed_id]}?lang=ru",
            callback=self.parse_ru_feed,
            meta={"feed_id": feed_id, "feed_index": feed_index},
            dont_filter=True,
        )

    def parse_ru_feed(self, response: Response) -> Iterator[scrapy.Request | dict]:
        """
        RU XML-фід → об'єднує з UA кешем → yield item напряму в pipeline.
        Немає Playwright, немає page requests.
        """
        feed_id: str = response.meta["feed_id"]
        feed_index: int = response.meta["feed_index"]

        if response.status != 200:
            self.logger.error(
                f"❌ RU фід {feed_id}: HTTP {response.status} — пропускаємо фід"
            )
        else:
            selector = Selector(text=response.text, type="xml")
            selector.remove_namespaces()
            self.logger.info(
                f"📂 [{feed_index + 1}/{len(self.ALL_FEED_IDS)}] RU фід {feed_id} — парсинг..."
            )

            total = yielded = deleted = skipped = 0
            unmapped: set[str] = set()

            for offer in selector.xpath("//offer"):
                product_id = offer.xpath("@id").get()
                category_id = offer.xpath("categoryId/text()").get() or ""

                if self._is_deleted_category(category_id):
                    deleted += 1
                    continue

                total += 1

                category_info = self._get_category_info(feed_id, category_id)
                if not category_info:
                    unmapped.add(category_id)

                ua_data = self.products_ua.get(product_id, {})

                name_ru = re.sub(
                    r'\s*\([A-Z0-9\.]+\)\s*$',
                    "",
                    offer.xpath("name/text()").get() or "",
                ).strip()

                item = self._build_item(
                    offer=offer,
                    feed_id=feed_id,
                    name_ru=name_ru,
                    name_ua=ua_data.get("name_ua", name_ru),
                    description_ru=self._clean_description(
                        offer.xpath("description/text()").get()
                    ),
                    description_ua=ua_data.get("description_ua", ""),
                    category_id=category_id,
                    category_info=category_info,
                )

                if item is None:
                    skipped += 1
                    continue

                product_url: str = item.get("Продукт_на_сайті", "")
                dedup_key = (feed_id, product_url)
                if dedup_key in self.processed_seen:
                    skipped += 1
                    self.logger.debug(f"⏭️ Дублікат [{feed_id}]: {product_url}")
                    continue
                self.processed_seen.add(dedup_key)

                specs: list[dict] = ua_data.get("specs_from_feed", [])
                specs = self.category_enricher.enrich_specs_by_category_id(
                    specs, category_id, feed_id
                )
                item["specifications_list"] = specs

                yielded += 1
                self.logger.info(
                    f"✅ {item.get('Назва_позиції', '')[:50]} | "
                    f"Dealer: {item.get('Ціна', '')} UAH | "
                    f"Specs: {len(specs)} | "
                    f"Photos: {len(item.get('Посилання_зображення', '').split(', ')) if item.get('Посилання_зображення') else 0}"
                )
                yield item

            self.logger.info(
                f"✅ Фід {feed_id}: {total} товарів | {yielded} → pipeline | "
                f"{deleted} видалено (уцінка) | {skipped} пропущено"
            )
            if unmapped:
                self.logger.warning(
                    f"⚠️ Фід {feed_id} — unmapped categories: {sorted(unmapped)}"
                )

        # Незалежно від результату поточного фіду — переходимо до наступного
        next_index = feed_index + 1
        if next_index < len(self.ALL_FEED_IDS):
            self.logger.info(
                f"➡️ Перехід до фіду [{next_index + 1}/{len(self.ALL_FEED_IDS)}]: "
                f"{self.ALL_FEED_IDS[next_index]}"
            )
            yield self._ua_request(feed_index=next_index)
        else:
            self.logger.info("🎉 Всі фіди прочитані!")

    # ──────────────────────────────────────────────────────────────────
    # ITEM BUILDING
    # ──────────────────────────────────────────────────────────────────

    def _build_item(
        self,
        offer,
        feed_id: str,
        name_ru: str,
        name_ua: str,
        description_ru: str,
        description_ua: str,
        category_id: str,
        category_info: dict,
    ) -> dict | None:
        """
        Формує item зі всіх полів одного <offer>.
        Повертає None якщо <price> відсутня (критичне поле).
        """
        product_id = offer.xpath("@id").get()

        retail_price, dealer_price = self._resolve_price(offer, feed_id)
        if dealer_price is None:
            self.logger.warning(
                f"⚠️ Пропускаємо id={product_id} (feed={feed_id}): відсутня <price>"
            )
            return None

        url = offer.xpath("url/text()").get() or ""
        available = offer.xpath("@available").get() == "true"
        vendor_code = (offer.xpath("vendorCode/text()").get() or "").strip()
        brand = (
            offer.xpath("brand/text()").get()
            or offer.xpath("vendor/text()").get()
            or ""
        ).strip()

        pictures = offer.xpath("picture/text()").getall()
        feed_image_url = ", ".join(pictures) if pictures else ""

        # available=True  → "" → pipeline підставить DEFAULT_QUANTITY
        # available=False → "0" → pipeline залишає як є
        quantity = "" if available else "0"

        return {
            "supplier_id":  self.supplier_id,
            "output_file":  self.output_filename,
            "price_type":   self.price_type,
            "source":       "feed",

            "Код_товару":           vendor_code or product_id,
            "Ідентифікатор_товару": product_id,

            "Назва_позиції":     name_ru,
            "Назва_позиції_укр": name_ua,
            "Опис":              description_ru,
            "Опис_укр":          description_ua,

            "Тип_товару":     "r",
            "Ціна":           str(dealer_price),
            "Валюта":         self.currency,
            "Одиниця_виміру": "шт.",

            "price_rrp_uah":    str(retail_price) if retail_price is not None else "",
            "dealer_price_uah": str(dealer_price),

            "Посилання_зображення": feed_image_url,
            "Наявність":            "В наявності" if available else "Немає в наявності",
            "Кількість":            quantity,

            "Виробник":        brand,
            "Країна_виробник": "",
            "Продукт_на_сайті": url,

            "Пошукові_запити":     "",
            "Пошукові_запити_укр": "",

            "Назва_групи":              category_info.get("category_name", ""),
            "Назва_групи_укр":          category_info.get("category_name", ""),
            "Номер_групи":              category_info.get("group_number", ""),
            "Ідентифікатор_підрозділу": category_info.get("subdivision_id", ""),
            "Посилання_підрозділу":     category_info.get("subdivision_link", ""),

            "feed_id":      feed_id,
            "category_url": category_info.get("category_url", ""),
            "category_id":  category_id,

            "specifications_list": [],  # заповнюється після enrich у parse_ru_feed
        }

    def _resolve_price(
        self, offer, feed_id: str
    ) -> tuple[float | None, float | None]:
        """
        Повертає (retail_price, dealer_price).

        retail  = <price>       (РРЦ)
        dealer  = <dealerPrice> (дилерська)

        Захист: dealerPrice > price → WARNING, dealer = retail.
        Fallback: dealerPrice відсутня → dealer = retail.
        Критично: price відсутня → (None, None).
        """
        retail = self._to_float(offer.xpath("price/text()").get())
        if retail is None:
            return None, None

        dealer_raw = self._to_float(offer.xpath("dealerPrice/text()").get())
        if dealer_raw is None:
            return retail, retail

        if dealer_raw > retail:
            self.logger.warning(
                f"⚠️ dealerPrice ({dealer_raw}) > price ({retail}) "
                f"id={offer.xpath('@id').get()} feed={feed_id} — fallback до price"
            )
            return retail, retail

        return retail, dealer_raw

    # ──────────────────────────────────────────────────────────────────
    # FEED SPECS PARSER
    # ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_feed_specs(offer) -> list[dict]:
        """
        Парсить <param> з UA XML-фіду (УКРАЇНСЬКА мова).

        XML приклад:
          <param name="Колір">Чорний</param>
          <param name="Вага" unit="кг">0.5</param>
        """
        return [
            {
                "name":  (param.xpath("@name").get() or "").strip(),
                "unit":  (param.xpath("@unit").get() or "").strip(),
                "value": (param.xpath("text()").get() or "").strip(),
            }
            for param in offer.xpath("param")
            if (param.xpath("@name").get() or "").strip()
            and (param.xpath("text()").get() or "").strip()
        ]

    # ──────────────────────────────────────────────────────────────────
    # UTILS
    # ──────────────────────────────────────────────────────────────────

    def closed(self, reason: str) -> None:
        self.logger.info(f"🎉 {self.name} завершено. Причина: {reason}")
        try:
            import winsound
            for _ in range(3):
                winsound.Beep(1000, 300)
        except Exception:
            pass  # не Windows або немає звуку — не критично

    @staticmethod
    def _to_float(value: str | None) -> float | None:
        if not value:
            return None
        try:
            return float(str(value).replace(",", ".").strip())
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _clean_description(description: str | None) -> str:
        if not description:
            return ""
        description = re.sub(r'<!\[CDATA\[|\]\]>', "", description)
        description = re.sub(r'\s*style="[^"]*"', "", description)
        description = re.sub(r'>\s+<', "><", description)
        if len(description) > 10_000:
            description = description[:10_000] + "...</p>"
        return description.strip()
