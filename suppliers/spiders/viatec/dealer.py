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
"""
import scrapy
import csv
from pathlib import Path
from urllib.parse import urljoin
import os
from dotenv import load_dotenv
from suppliers.spiders.base import ViatecBaseSpider, BaseDealerSpider
from suppliers.services.category_specs_enricher import CategorySpecsEnricher
from suppliers.services.viatec_feed_service import ViatecFeedService

PRIORITY_PRODUCT  = 10
PRIORITY_CATEGORY = 0


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

        _project_root = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
        load_dotenv(_project_root / "suppliers" / ".env")
        self.email    = os.getenv("VIATEC_EMAIL")
        self.password = os.getenv("VIATEC_PASSWORD")

        if not self.email or not self.password:
            raise ValueError(
                "❌ Відсутні VIATEC_EMAIL / VIATEC_PASSWORD. "
                "Локально: додайте в suppliers/.env. CI: додайте в GitHub Secrets."
            )

        self.category_mapping = self._load_category_mapping()
        self.category_urls    = list(self.category_mapping.keys())

        _root = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
        csv_path = str(_root / "data" / "viatec" / "viatec_category.csv")
        self.category_enricher = CategorySpecsEnricher(csv_path, self.supplier_id)

        # ── XML-фід: виробники за артикулом (пріоритет перед CSV-словариком) ──
        self.feed_service = ViatecFeedService(logger=self.logger)

        # ── RESUME: завантажуємо вже спарсені товари з попереднього запуску ──
        already_scraped = self._load_already_scraped_urls(_root)
        self.processed_products.update(already_scraped)

    # ──────────────────────────────────────────────────────────
    # RESUME
    # ──────────────────────────────────────────────────────────

    def _load_already_scraped_urls(self, root: Path) -> set:
        """
        Читає вже збережений viatec_new.csv і повертає set URL.
        Якщо файл не існує — порожній set (перший запуск).
        Ідентично secur::_load_already_scraped_urls()
        """
        urls: set = set()
        out_path = root / "data" / "output" / self.output_filename
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
        except Exception as e:
            self.logger.warning(f"⚠️  Не вдалося завантажити resume CSV: {e}")
        return urls

    # ──────────────────────────────────────────────────────────
    # CATEGORY MAPPING
    # ──────────────────────────────────────────────────────────

    def _load_category_mapping(self):
        mapping = {}
        _root    = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
        csv_path = _root / "data" / "viatec" / "viatec_category.csv"
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
            self.logger.info(f"✅ Завантажено {len(mapping)} категорій (site channel)")
        except Exception as e:
            self.logger.error(f"❌ Помилка завантаження категорій: {e}")
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
            meta={"category_url": first, "category_index": 0, "page_number": 1},
            priority=PRIORITY_CATEGORY,
            dont_filter=True,
        )

    # ──────────────────────────────────────────────────────────
    # PARSE CATEGORY
    # ──────────────────────────────────────────────────────────

    def parse_category(self, response):
        category_url   = response.meta["category_url"]
        category_index = response.meta["category_index"]
        page_number    = response.meta.get("page_number", 1)
        category_info  = self.category_mapping.get(category_url, {})

        self.logger.info(
            f"📂 Категорія [{category_index + 1}/{len(self.category_urls)}] "
            f"стор.{page_number}"
        )

        product_links = response.css("a[href*='/product/']::attr(href)").getall()
        new_count = 0

        if not product_links:
            self.logger.warning(f"⚠️ Не знайдено товарів: {response.url}")
        else:
            for i, link in enumerate(product_links):
                normalized_url = response.urljoin(link).replace("/ru/", "/")
                if normalized_url in self.processed_products:
                    continue
                self.processed_products.add(normalized_url)
                new_count += 1
                yield scrapy.Request(
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
                    priority=PRIORITY_PRODUCT + len(product_links) - i,
                    dont_filter=True,
                )
            if new_count:
                self.logger.info(f"   ➕ Додано в чергу: {new_count} товарів")

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

            supplier_sku = (response.css("span.card-header__card-articul-text-value::text").get() or "").strip()
            price_raw    = (response.css("div.card-header__card-price-new::text").get() or "").strip().replace("&nbsp;", "").replace(" ", "")
            price        = self._clean_price(price_raw) if price_raw else ""

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
                "price_type":               self.price_type,
                "supplier_id":              self.supplier_id,
                "output_file":              self.output_filename,
                "Продукт_на_сайті":         response.meta.get("original_url", response.url),
                "category_url":             response.meta.get("category_url", ""),
                "specifications_list":      specs_list,
            }
            yield item
        except Exception as e:
            self.logger.error(f"❌ Помилка парсингу (RU): {response.url} | {e}")

    def parse_product_error(self, failure):
        url          = failure.request.url
        product_name = failure.request.meta.get("name_ua", "Назва не знайдена")
        self.logger.error(f"❌ Помилка товару: {product_name} ({url}). {failure.value}")
        self.failed_products.append({
            "url": url, "reason": str(failure.value), "product_name": product_name
        })
        # Scrapy автоматично бере наступний з черги — нічого більше не потрібно

    # ──────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────

    def _start_next_category(self, current_index: int):
        next_index = current_index + 1
        if next_index >= len(self.category_urls):
            self.logger.info("✅ ВСІ КАТЕГОРІЇ ОБРОБЛЕНІ")
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
