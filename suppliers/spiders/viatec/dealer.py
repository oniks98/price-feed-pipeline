"""
Spider для парсингу дилерських цін з viatec.ua (USD)
Потребує авторизації через форму логіну
Вигружає дані в: output/viatec_new.csv

ПОСЛІДОВНА ОБРОБКА: категорія → всі сторінки пагінації → наступна категорія
ХАРАКТЕРИСТИКИ: парсяться УКРАЇНСЬКОЮ (UA) мовою з підтримкою rule_kind
МУЛЬТИКАНАЛЬНИЙ РЕЖИМ: підтримка каналів site, prom з viatec_category.csv

РЕФАКТОРИНГ:
- priority замість remaining_products — прибирає зростання пам'яті
  Scrapy сам керує чергою; _skip_product більше не потрібен
- USD курс: парсинг з навігації (lk-nav), fallback → DEFAULT_USD_RATE
"""
import scrapy
import csv
import re
from decimal import Decimal
from pathlib import Path
from urllib.parse import urljoin
import os
from dotenv import load_dotenv
from suppliers.spiders.base import ViatecBaseSpider, BaseDealerSpider
from suppliers.services.category_specs_enricher import CategorySpecsEnricher
from suppliers.services.viatec_feed_service import ViatecFeedService
from suppliers.services.dealer_price_service import (
    DealerPriceService as ViatecPriceService,
    DEFAULT_USD_RATE,
    VIATEC_PROM_THRESHOLD,
    VIATEC_SITE_THRESHOLD,
)
from suppliers.services.channel_service import ChannelService

PRIORITY_PRODUCT  = 10
PRIORITY_CATEGORY = 0
RAW_CSV_ROWS_FIELD = "__raw_csv_rows__"


def _base_sku(identifier: str) -> str:
    sku = (identifier or "").strip()
    return sku[5:] if sku.startswith("prom_") else sku


def _compact_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def _normalize_card_availability(status_raw: str | None) -> tuple[str, str] | None:
    status = _compact_text(status_raw)
    if not status:
        return None

    quantity_match = re.search(r"\b(\d+)\s*шт\b", status, re.IGNORECASE)
    if quantity_match and status.startswith("В наявності"):
        return "+", quantity_match.group(1)

    if status in {"В наявності", "Закінчується"}:
        return "+", "10000"

    return None


def _parse_card_price_usd(raw: str | None) -> str:
    text = _compact_text(raw).replace("\xa0", " ").replace(",", ".")
    match = re.search(r"\d+(?:\s\d{3})*(?:\.\d+)?", text)
    if not match:
        return ""
    return match.group(0).replace(" ", "")


def _parse_card_rrp_uah(raw: str | None) -> str:
    return _parse_card_price_usd(raw)


class ViatecDealerSpider(ViatecBaseSpider, BaseDealerSpider):
    name = "viatec_dealer"
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        import logging
        logging.getLogger("scrapy.crawler").setLevel(logging.WARNING)

        _project_root = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
        load_dotenv(_project_root / "suppliers" / ".env")
        self.email    = os.getenv("VIATEC_EMAIL")
        self.password = os.getenv("VIATEC_PASSWORD")

        if not self.email or not self.password:
            raise ValueError(
                "❌ Відсутні VIATEC_EMAIL / VIATEC_PASSWORD. "
                "Локально: додайте в suppliers/.env. CI: додайте в GitHub Secrets."
            )

        # USD курс: дефолт до парсингу зі сторінки
        self.usd_rate: Decimal = DEFAULT_USD_RATE

        self._project_root = _project_root
        self.category_mapping = self._load_category_mapping()
        self.category_urls    = list(self.category_mapping.keys())

        _root = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
        csv_path = str(_root / "data" / "viatec" / "viatec_category.csv")
        try:
            self.category_enricher = CategorySpecsEnricher(csv_path, self.supplier_id)
        except Exception as exc:
            raise RuntimeError(f"❌ CategorySpecsEnricher не ініціалізовано: {exc}") from exc
        self.fast_channel_service = ChannelService(Path(csv_path), self.logger, decimal_places=0)

        # ── XML-фід: виробники за артикулом (пріоритет перед CSV-словариком) ──
        try:
            self.feed_service = ViatecFeedService(logger=self.logger)
        except Exception as exc:
            raise RuntimeError(f"❌ ViatecFeedService не ініціалізовано: {exc}") from exc

        # OLD index: готові CSV-рядки за SKU для fast-path з категорії.
        self.old_headers, self.old_index = self._load_old_products_index(_root)

        # ── RESUME: завантажуємо вже спарсені товари з попереднього запуску ──
        self.processed_skus: set[str] = set()
        already_scraped_urls, already_scraped_skus = self._load_already_scraped_state(_root)
        self.processed_products.update(already_scraped_urls)
        self.processed_skus.update(already_scraped_skus)

        self.fast_reused_count = 0
        self.fast_updated_count = 0
        self.full_parse_count = 0
        self.skipped_not_available_count = 0

    # ──────────────────────────────────────────────────────────
    # RESUME
    # ──────────────────────────────────────────────────────────

    def _load_already_scraped_state(self, root: Path) -> tuple[set[str], set[str]]:
        """
        Читає вже збережений viatec_new.csv і повертає set URL + set SKU.
        Якщо файл не існує — порожні set-и (перший запуск).
        """
        urls: set[str] = set()
        skus: set[str] = set()
        out_path = root / "data" / "output" / self.output_filename
        if not out_path.exists():
            return urls, skus
        try:
            with open(out_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    url = row.get("Продукт_на_сайті", "").strip()
                    if url:
                        urls.add(url)
                    sku = _base_sku(row.get("Ідентифікатор_товару", ""))
                    if sku:
                        skus.add(sku)
            self.logger.info(
                f"📋 Resume: знайдено {len(urls)} URL / {len(skus)} SKU — пропускаємо"
            )
        except Exception as e:
            self.logger.warning(f"⚠️  Не вдалося завантажити resume CSV: {e}")
        return urls, skus

    def _load_old_products_index(self, root: Path) -> tuple[list[str], dict[str, dict]]:
        """
        Індексує data/viatec/viatec_old.csv за базовим SKU.

        У файлі два канали на товар: SKU і prom_SKU. Для fast-path зберігаємо
        обидва готові рядки та оновлюємо в них тільки поля, які видно в категорії.
        """
        path = root / "data" / "viatec" / "viatec_old.csv"
        if not path.exists():
            self.logger.warning(f"⚠️ OLD CSV не знайдено, fast-path вимкнено: {path}")
            return [], {}

        try:
            with open(path, encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f, delimiter=";")
                headers = next(reader, [])
                identifier_idx = self._field_index(headers, "Ідентифікатор_товару")
                wholesale_idx = self._field_index(headers, "Оптова_ціна")
                url_idx = self._field_index(headers, "Продукт_на_сайті")

                if identifier_idx == -1 or wholesale_idx == -1:
                    self.logger.warning(
                        "⚠️ OLD CSV не має Ідентифікатор_товару/Оптова_ціна, fast-path вимкнено"
                    )
                    return headers, {}

                index: dict[str, dict] = {}
                for row in reader:
                    row = self._ensure_row_len(row, len(headers))
                    identifier = row[identifier_idx].strip()
                    sku = _base_sku(identifier)
                    if not sku:
                        continue

                    entry = index.setdefault(
                        sku,
                        {"rows": [], "wholesale": "", "url": ""},
                    )
                    entry["rows"].append(row)

                    if not identifier.startswith("prom_") or not entry["wholesale"]:
                        entry["wholesale"] = row[wholesale_idx].strip()
                    if url_idx != -1 and row[url_idx].strip():
                        entry["url"] = row[url_idx].strip()

            self.logger.info(f"⚡ Fast-path OLD index: {len(index)} SKU з {path.name}")
            return headers, index
        except Exception as exc:
            self.logger.warning(f"⚠️ Не вдалося завантажити OLD index: {exc}")
            return [], {}

    # ──────────────────────────────────────────────────────────
    # CATEGORY MAPPING
    # ──────────────────────────────────────────────────────────

    def _load_category_mapping(self):
        mapping = {}
        _root    = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
        csv_path = _root / "data" / "viatec" / "viatec_category.csv"
        if not csv_path.exists():
            raise RuntimeError(f"❌ Файл категорій не знайдено: {csv_path}")
        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    if row.get("channel", "").strip() != "site":
                        continue
                    url = row["Линк категории поставщика"].strip().strip('"')
                    if not url or not url.startswith("http"):
                        continue
                    mapping[url] = {
                        "category_ru":      row.get("Назва_групи", ""),
                        "category_ua":      row.get("Назва_групи", ""),
                        "group_number":     row.get("Номер_групи", ""),
                        "subdivision_id":   row.get("Ідентифікатор_підрозділу", ""),
                        "subdivision_link": row.get("Посилання_підрозділу", ""),
                    }
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"❌ Не вдалося завантажити category mapping: {exc}") from exc
        if not mapping:
            raise RuntimeError(f"❌ Category mapping порожній (немає site-рядків): {csv_path}")
        self.logger.info(f"✅ Завантажено {len(mapping)} категорій (site channel)")
        return mapping

    # ──────────────────────────────────────────────────────────
    # AUTH
    # ──────────────────────────────────────────────────────────

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
            formdata={"_token": csrf, "email": self.email, "password": self.password},
            callback=self.after_login,
            dont_filter=True,
        )

    def after_login(self, response):
        if b"viatec_session" not in b" ".join(response.headers.getlist("Set-Cookie")):
            self.logger.error("Авторизація не виконана!")
            return
        self.logger.info("✅ УСПІШНИЙ ЛОГІН")

        if not self.category_urls:
            self.logger.error("Немає категорій для парсингу.")
            return
        first = self.category_urls[0]
        yield scrapy.Request(
            url=first,
            callback=self.parse_category,
            errback=self.parse_category_error,
            meta={"category_url": first, "category_index": 0, "page_number": 1},
            priority=PRIORITY_CATEGORY,
            dont_filter=True,
        )

    # ──────────────────────────────────────────────────────────
    # USD RATE
    # ──────────────────────────────────────────────────────────

    def _try_update_usd_rate(self, response, source: str = "") -> None:
        """
        Оновлює self.usd_rate курсом USD б/г зі сторінки.
        Логує тільки при зміні курсу. При невдачі — мовчки залишає поточний.
        """
        rate = ViatecPriceService.parse_usd_rate_from_response(response)
        if rate is not None and rate != self.usd_rate:
            self.usd_rate = rate
            self.logger.info(f"💱 USD б/г курс: {self.usd_rate} ({source})")

    # ──────────────────────────────────────────────────────────
    # PARSE CATEGORY
    # ──────────────────────────────────────────────────────────

    def parse_category(self, response):
        category_url   = response.meta["category_url"]
        category_index = response.meta["category_index"]
        page_number    = response.meta.get("page_number", 1)
        category_info  = self.category_mapping.get(category_url, {})

        # Оновлюємо курс якщо ще не отримали зі сторінки після логіну
        if self.usd_rate == DEFAULT_USD_RATE:
            self._try_update_usd_rate(response, source="parse_category")

        self.logger.info(
            f"📂 Категорія [{category_index + 1}/{len(self.category_urls)}] "
            f"стор.{page_number}"
        )

        product_links = response.css("a[href*='/product/']::attr(href)").getall()
        page_full_count = 0
        page_fast_reused = 0
        page_fast_updated = 0
        page_skipped = 0
        seen_card_urls: set[str] = set()

        if not product_links:
            self.logger.warning(f"⚠️ Не знайдено товарів: {response.url}")
        else:
            category_cards = self._extract_category_cards(response)

            for i, card in enumerate(category_cards):
                normalized_url = card["url"]
                if normalized_url:
                    seen_card_urls.add(normalized_url)

                result = self._handle_category_card(
                    card=card,
                    category_info=category_info,
                    category_url=category_url,
                    priority=PRIORITY_PRODUCT + len(product_links) - i,
                )
                if result is None:
                    continue

                kind, payload = result
                if kind == "skip":
                    page_skipped += 1
                    self.skipped_not_available_count += 1
                    continue
                if kind == "fast_reused":
                    page_fast_reused += 1
                    self.fast_reused_count += 1
                    yield payload
                    continue
                if kind == "fast_updated":
                    page_fast_updated += 1
                    self.fast_updated_count += 1
                    yield payload
                    continue
                if kind == "request":
                    if payload:
                        page_full_count += 1
                        self.full_parse_count += 1
                        yield payload

            for i, link in enumerate(product_links):
                normalized_url = response.urljoin(link).replace("/ru/", "/")
                if normalized_url in seen_card_urls:
                    continue
                request = self._schedule_product_request(
                    normalized_url=normalized_url,
                    category_info=category_info,
                    category_url=category_url,
                    priority=PRIORITY_PRODUCT + len(product_links) - i,
                )
                if request:
                    page_full_count += 1
                    self.full_parse_count += 1
                    yield request

            if page_full_count or page_fast_reused or page_fast_updated or page_skipped:
                self.logger.info(
                    "   ⚡ category fast-path: "
                    f"old={page_fast_reused}, updated={page_fast_updated}, "
                    f"full={page_full_count}, skipped_no_stock={page_skipped}"
                )

        # Пагінація
        next_page_link = response.css("a.paggination__next::attr(href)").get()
        if not next_page_link:
            all_pages          = response.css("a.paggination__page::attr(href)").getall()
            active_page_nodes  = response.css("a.paggination__page--active")
            if all_pages and active_page_nodes:
                try:
                    active_text    = active_page_nodes[0].css("::text").get()
                    all_texts      = [a.css("::text").get() for a in response.css("a.paggination__page")]
                    current_idx    = all_texts.index(active_text)
                    if 0 <= current_idx + 1 < len(all_pages):
                        next_page_link = all_pages[current_idx + 1]
                except (ValueError, IndexError):
                    pass

        if next_page_link:
            yield scrapy.Request(
                url=urljoin(response.url, next_page_link),
                callback=self.parse_category,
                errback=self.parse_category_error,
                meta={
                    "category_url":   category_url,
                    "category_index": category_index,
                    "page_number":    page_number + 1,
                },
                priority=PRIORITY_CATEGORY,
                dont_filter=True,
            )
        else:
            self.logger.info(
                f"✅ ПАГІНАЦІЯ ЗАВЕРШЕНА [{category_index + 1}/{len(self.category_urls)}]"
            )
            next_cat = self._start_next_category(category_index)
            if next_cat:
                yield next_cat

    # ──────────────────────────────────────────────────────────
    # PARSE PRODUCT
    # ──────────────────────────────────────────────────────────

    def _handle_category_card(
        self,
        card: dict,
        category_info: dict,
        category_url: str,
        priority: int,
    ) -> tuple[str, object] | None:
        normalized_url = card.get("url", "")
        sku = _base_sku(card.get("sku", ""))

        if not normalized_url:
            return None
        if normalized_url in self.processed_products or (sku and sku in self.processed_skus):
            return None

        availability_raw = card.get("availability", "")
        availability_data = _normalize_card_availability(availability_raw)
        if availability_data is None:
            if not _compact_text(availability_raw):
                return "request", self._schedule_product_request(
                    normalized_url=normalized_url,
                    category_info=category_info,
                    category_url=category_url,
                    priority=priority,
                    sku=sku,
                )
            self.processed_products.add(normalized_url)
            if sku:
                self.processed_skus.add(sku)
            return "skip", None

        if not sku or not card.get("price"):
            return "request", self._schedule_product_request(
                normalized_url=normalized_url,
                category_info=category_info,
                category_url=category_url,
                priority=priority,
                sku=sku,
            )

        old_entry = self.old_index.get(sku)
        if not old_entry:
            return "request", self._schedule_product_request(
                normalized_url=normalized_url,
                category_info=category_info,
                category_url=category_url,
                priority=priority,
                sku=sku,
            )

        self.processed_products.add(normalized_url)
        self.processed_skus.add(sku)

        availability, quantity = availability_data
        wholesale = self._dealer_wholesale_uah(card["price"])
        rows = self._build_fast_rows(
            old_entry=old_entry,
            category_url=category_url,
            availability=availability,
            quantity=quantity,
            wholesale=wholesale,
            price_rrp_uah=card.get("price_rrp_uah", ""),
            product_url=normalized_url,
        )

        item = {
            RAW_CSV_ROWS_FIELD: rows,
            "output_file": self.output_filename,
        }
        kind = (
            "fast_reused"
            if self._prices_equal(old_entry.get("wholesale", ""), wholesale)
            else "fast_updated"
        )
        return kind, item

    def _schedule_product_request(
        self,
        normalized_url: str,
        category_info: dict,
        category_url: str,
        priority: int,
        sku: str = "",
    ):
        if normalized_url in self.processed_products or (sku and sku in self.processed_skus):
            return None
        self.processed_products.add(normalized_url)
        if sku:
            self.processed_skus.add(sku)

        return scrapy.Request(
            url=normalized_url,
            callback=self.parse_product,
            errback=self.parse_product_error,
            meta={
                "category_url":     category_url,
                "category_ru":      category_info.get("category_ru", ""),
                "category_ua":      category_info.get("category_ua", ""),
                "group_number":     category_info.get("group_number", ""),
                "subdivision_id":   category_info.get("subdivision_id", ""),
                "subdivision_link": category_info.get("subdivision_link", ""),
            },
            priority=priority,
            dont_filter=True,
        )

    def parse_product(self, response):
        try:
            name_ua        = (response.css("h1::text").get() or "").strip()
            description_ua = self._extract_description_with_br(response)
            specs_list_ua  = self._extract_specifications(response)
            category_url   = response.meta.get("category_url", "")
            if category_url:
                specs_list_ua = self.category_enricher.enrich_specs(specs_list_ua, category_url)
            ru_url         = self._convert_to_ru_url(response.url)
            yield scrapy.Request(
                url=ru_url,
                callback=self.parse_product_ru,
                errback=self.parse_product_error,
                meta={
                    **response.meta,
                    "name_ua":             name_ua,
                    "description_ua":      description_ua,
                    "specifications_list": specs_list_ua,
                    "original_url":        response.url,
                },
                priority=PRIORITY_PRODUCT,
                dont_filter=True,
            )
        except Exception as e:
            self.logger.error(f"❌ Помилка парсингу (UA): {response.url} | {e}")

    def parse_product_ru(self, response):
        try:
            name_ru        = (response.css("h1::text").get() or "").strip()
            description_ru = self._extract_description_with_br(response)
            name_ua        = response.meta.get("name_ua", "")
            description_ua = response.meta.get("description_ua", "")
            specs_list     = response.meta.get("specifications_list", [])

            supplier_sku  = (response.css("span.card-header__card-articul-text-value::text").get() or "").strip()
            price_raw     = (response.css("div.card-header__card-price-new::text").get() or "").strip().replace("&nbsp;", "").replace(" ", "")
            price         = self._clean_price(price_raw) if price_raw else ""
            price_rrp_uah = self._parse_rrp_uah(response)

            gallery_images = response.css('a[data-fancybox*="gallery"]::attr(href)').getall()
            if not gallery_images:
                gallery_images = response.css("img.card-header__card-images-image::attr(src)").getall()
            image_urls = [
                s for img in gallery_images
                if (s := self._sanitize_image_url(response.urljoin(img)))
            ]
            image_url = ", ".join(image_urls)

            availability_raw = response.css("div.card-header__card-status-badge::text").get()
            availability     = self._normalize_availability(availability_raw)
            quantity         = self._extract_quantity(availability_raw)

            item = {
                "Код_товару":               "",
                "Назва_позиції":            name_ru,
                "Назва_позиції_укр":        name_ua,
                "Пошукові_запити":          "",
                "Пошукові_запити_укр":      "",
                "Опис":                     description_ru,
                "Опис_укр":                 description_ua,
                "Тип_товару":               "r",
                "Ціна":                     price,
                "Валюта":                   self.currency,
                "Одиниця_виміру":           "шт.",
                "Посилання_зображення":     image_url,
                "Наявність":                availability,
                "Кількість":                quantity,
                "Назва_групи":              response.meta.get("category_ru", ""),
                "Назва_групи_укр":          response.meta.get("category_ua", ""),
                "Номер_групи":              response.meta.get("group_number", ""),
                "Ідентифікатор_товару":     supplier_sku,
                "Ідентифікатор_підрозділу": response.meta.get("subdivision_id", ""),
                "Посилання_підрозділу":     response.meta.get("subdivision_link", ""),
                "Виробник":                 self.feed_service.get_vendor(supplier_sku),
                "Країна_виробник":          "",
                "price_rrp_uah":            price_rrp_uah,
                "price_type":               self.price_type,
                "supplier_id":              self.supplier_id,
                # USD курс на момент парсингу — pipeline використовує для конвертації
                "usd_rate":                 str(self.usd_rate),
                "output_file":              self.output_filename,
                "Продукт_на_сайті":         response.meta.get("original_url", response.url),
                "category_url":             response.meta.get("category_url", ""),
                "specifications_list":      specs_list,
            }
            yield item
        except Exception as e:
            self.logger.error(f"❌ Помилка парсингу (RU): {response.url} | {e}")

    def parse_category_error(self, failure):
        """Категорія впала після всіх Scrapy-ретраїв → переходимо до наступної."""
        request        = failure.request
        category_index = request.meta.get("category_index", 0)
        page_number    = request.meta.get("page_number", 1)
        self.logger.error(
            f"❌ Категорія [{category_index + 1}/{len(self.category_urls)}] "
            f"стор.{page_number} недоступна після всіх ретраїв: {request.url} "
            f"| {failure.value} — переходимо до наступної"
        )
        next_cat = self._start_next_category(category_index)
        if next_cat:
            return next_cat

    def parse_product_error(self, failure):
        url          = failure.request.url
        product_name = failure.request.meta.get("name_ua", "Назва не знайдена")
        self.logger.error(f"❌ Помилка товару: {product_name} ({url}). {failure.value}")
        self.failed_products.append({
            "url": url, "reason": str(failure.value), "product_name": product_name
        })

    # ──────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────

    def _extract_category_cards(self, response) -> list[dict[str, str]]:
        cards: list[dict[str, str]] = []
        for sku_node in response.css("p.categories__item-code"):
            card = sku_node.xpath("./ancestor::*[.//a[contains(@href, '/product/')]][1]")
            if not card:
                continue

            link = card.css("a[href*='/product/']::attr(href)").get()
            if not link:
                continue

            price = ""
            for price_block in card.css("p.categories__item-bn-price"):
                price_text = _compact_text(" ".join(price_block.css("::text").getall()))
                if "ціна по б/г" not in price_text:
                    continue
                raw_price = price_block.css("span.color-main.bold::text").get("")
                price = _parse_card_price_usd(raw_price)
                break

            price_rrp_uah = ""
            for rrp_node in card.css("p.color-gray-80.font-0-8, p.font-0-8"):
                rrp_text = _compact_text(" ".join(rrp_node.css("::text").getall()))
                if "РРЦ" not in rrp_text:
                    continue
                price_rrp_uah = _parse_card_rrp_uah(rrp_text)
                break

            availability = _compact_text(
                " ".join(
                    card.css(
                        "div.card-header__card-status-text::text, "
                        "div.card-header__card-status-text *::text"
                    ).getall()
                )
            )

            cards.append({
                "url": response.urljoin(link).replace("/ru/", "/"),
                "sku": _compact_text(sku_node.css("::text").get("")),
                "price": price,
                "price_rrp_uah": price_rrp_uah,
                "availability": availability,
            })
        return cards

    def _build_fast_rows(
        self,
        old_entry: dict,
        category_url: str,
        availability: str,
        quantity: str,
        wholesale: str,
        price_rrp_uah: str,
        product_url: str,
    ) -> list[list[str]]:
        identifier_idx = self._field_index(self.old_headers, "Ідентифікатор_товару")
        availability_idx = self._field_index(self.old_headers, "Наявність")
        quantity_idx = self._field_index(self.old_headers, "Кількість")
        price_idx = self._field_index(self.old_headers, "Ціна")
        wholesale_idx = self._field_index(self.old_headers, "Оптова_ціна")
        url_idx = self._field_index(self.old_headers, "Продукт_на_сайті")

        rows: list[list[str]] = []
        for old_row in old_entry.get("rows", []):
            row = self._ensure_row_len(old_row.copy(), len(self.old_headers))
            old_wholesale = row[wholesale_idx] if wholesale_idx != -1 else ""
            if price_idx != -1 and wholesale_idx != -1:
                channel_config = self._channel_config_for_fast_row(row, identifier_idx, category_url)
                adjusted_price = self._fast_channel_price(wholesale, price_rrp_uah, channel_config)
                if not adjusted_price:
                    adjusted_price = self._adjust_channel_price(
                        old_price=row[price_idx],
                        old_wholesale=old_wholesale,
                        new_wholesale=wholesale,
                    )
                if adjusted_price:
                    row[price_idx] = adjusted_price
            if availability_idx != -1:
                row[availability_idx] = availability
            if quantity_idx != -1:
                row[quantity_idx] = quantity
            if wholesale_idx != -1:
                row[wholesale_idx] = wholesale
            if url_idx != -1 and product_url:
                row[url_idx] = product_url
            rows.append(row)
        return rows

    def _dealer_wholesale_uah(self, dealer_usd: str) -> str:
        dealer_uah = ViatecPriceService.dealer_uah(dealer_usd, self.usd_rate)
        return ViatecPriceService.format_price(dealer_uah)

    def _channel_config_for_fast_row(self, row: list[str], identifier_idx: int, category_url: str):
        if identifier_idx == -1:
            return None

        identifier = row[identifier_idx].strip()
        if not identifier:
            return None

        channels = self.fast_channel_service.resolve_channels(category_url)
        for channel_config in sorted(channels, key=lambda c: len(c.prefix or ""), reverse=True):
            if channel_config.prefix:
                if identifier.startswith(channel_config.prefix):
                    return channel_config
            elif identifier == _base_sku(identifier):
                return channel_config
        return None

    def _fast_channel_price(self, wholesale: str, price_rrp_uah: str, channel_config) -> str:
        if channel_config is None:
            return ""

        dealer_uah = ViatecPriceService.to_decimal(wholesale, Decimal("0"))
        if dealer_uah <= 0:
            return ""

        price = ViatecPriceService.channel_price_for_config(
            channel_config=channel_config,
            retail_uah=price_rrp_uah,
            dealer_uah_val=dealer_uah,
            prom_threshold=VIATEC_PROM_THRESHOLD,
            site_threshold=VIATEC_SITE_THRESHOLD,
        )
        return ViatecPriceService.format_price(price)

    def _adjust_channel_price(self, old_price: str, old_wholesale: str, new_wholesale: str) -> str:
        """
        Fallback якщо для рядка не знайдено channel config: зберігає стару маржу.
        """
        old_price_dec = ViatecPriceService.to_decimal(old_price, Decimal("0"))
        old_wholesale_dec = ViatecPriceService.to_decimal(old_wholesale, Decimal("0"))
        new_wholesale_dec = ViatecPriceService.to_decimal(new_wholesale, Decimal("0"))
        if old_price_dec <= 0 or old_wholesale_dec <= 0 or new_wholesale_dec <= 0:
            return ""
        return ViatecPriceService.format_price(
            old_price_dec * new_wholesale_dec / old_wholesale_dec
        )

    def _prices_equal(self, left: str, right: str) -> bool:
        left_dec = ViatecPriceService.to_decimal(left, Decimal("0"))
        right_dec = ViatecPriceService.to_decimal(right, Decimal("0"))
        return left_dec == right_dec

    @staticmethod
    def _field_index(headers: list[str], field_name: str) -> int:
        try:
            return headers.index(field_name)
        except ValueError:
            return -1

    @staticmethod
    def _ensure_row_len(row: list[str], target_len: int) -> list[str]:
        if len(row) < target_len:
            return row + [""] * (target_len - len(row))
        if len(row) > target_len:
            return row[:target_len]
        return row

    def _parse_rrp_uah(self, response) -> str:
        """
        Парсить ціну РРЦ в гривнях зі сторінки товару.

        Шукає тег виду:
            <p class="font-0-9 color-gray-80 mb-1">2 099.00 грн (РРЦ)</p>

        Повертає очищену числову рядок (напр. "2099.00") або "" якщо не знайдено.
        Використовується каналом prom як базова ціна у UAH.
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

    def _start_next_category(self, current_index: int):
        next_index = current_index + 1
        if next_index >= len(self.category_urls):
            self.logger.info("✅ ВСІ КАТЕГОРІЇ ОБРОБЛЕНІ")
            self.logger.info(
                "⚡ Fast-path summary: "
                f"old={self.fast_reused_count}, updated={self.fast_updated_count}, "
                f"full={self.full_parse_count}, skipped_no_stock={self.skipped_not_available_count}"
            )
            return None
        next_url = self.category_urls[next_index]
        self.logger.info(f"🚀 НАСТУПНА КАТЕГОРІЯ [{next_index + 1}/{len(self.category_urls)}]")
        return scrapy.Request(
            url=next_url,
            callback=self.parse_category,
            meta={"category_url": next_url, "category_index": next_index, "page_number": 1},
            priority=PRIORITY_CATEGORY,
            dont_filter=True,
        )
