"""
Spider для парсингу роздрібних цін з secur.ua (UAH)
Вигружає дані в: output/secur_new.csv

ПОСЛІДОВНА ОБРОБКА: категорія → всі сторінки пагінації → наступна категорія
ХАРАКТЕРИСТИКИ: парсяться УКРАЇНСЬКОЮ (UA) та РОСІЙСЬКОЮ (RU) з окремих URL
МУЛЬТИКАНАЛЬНИЙ РЕЖИМ: підтримка каналів site, prom з secur_category.csv

АНТИЗАВИСАННЯ:
  1. goto timeout 30s + download_timeout 35s — Playwright не може висіти вічно
  2. priority замість remaining_products — прибирає зростання пам'яті
  3. Resume з _new.csv — при перезапуску продовжує з місця зупинки
  4. CONCURRENT_REQUESTS=2 — один зависший запит не вбиває весь паук
  5. User-Agent Chrome 133 + Accept-Language — знижує ймовірність soft-block

ПРИМІТКА: playwright_include_page + page.close() прибрано —
  scrapy-playwright сам керує lifecycle сторінок.
  Ручне закриття конфліктує з pending route handlers → AssertionError.

ВИПРАВЛЕНО:
- ЗАВДАННЯ 1: Регекс для видалення коду постачальника в дужках з кінця назви
- ЗАВДАННЯ 4: Парсинг великих зображень (/images/big/) замість маленьких preview
- ЗАВДАННЯ 5: Парсинг коду товару (Ідентифікатор_товару)
"""
import scrapy
import csv
import re
from pathlib import Path
from scrapy_playwright.page import PageMethod
from suppliers.spiders.base import BaseRetailSpider
from suppliers.services.category_specs_enricher import CategorySpecsEnricher

# ─────────────────────────────────────────────────────────────
# Константи Playwright
# ─────────────────────────────────────────────────────────────
GOTO_TIMEOUT_MS  = 30_000   # page.goto() timeout — 30 сек
DOWNLOAD_TIMEOUT = 35       # Scrapy-рівень — 35 сек (трохи більше goto)
JS_WAIT_MS       = 2_000    # secur.ua рендерить ~142 характеристики через JS
                             # без цієї паузи паук бачить лише ~17 з HTML

PRIORITY_PRODUCT  = 10
PRIORITY_CATEGORY = 0


def _playwright_meta(extra: dict | None = None) -> dict:
    """
    Playwright-мета з жорсткими timeout-ами.
    Єдина функція для всіх запитів — категорій і товарів.
    playwright_include_page НЕ використовується: scrapy-playwright
    сам закриває сторінки; ручне закриття ламає pending route handlers.
    """
    base = {
        "playwright": True,
        "download_timeout": DOWNLOAD_TIMEOUT,
        "playwright_page_goto_kwargs": {
            "wait_until": "domcontentloaded",
            "timeout": GOTO_TIMEOUT_MS,
        },
        "playwright_page_methods": [
            PageMethod("wait_for_timeout", JS_WAIT_MS),
        ],
    }
    if extra:
        base.update(extra)
    return base


class SecurRetailSpider(BaseRetailSpider):
    name = "secur_retail"
    supplier_id = "secur"
    output_filename = "secur_new.csv"
    allowed_domains = ["secur.ua"]

    custom_settings = {
        "ITEM_PIPELINES": {
            "suppliers.pipelines.SuppliersPipeline": 300,
        },
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 3,
        "RANDOMIZE_DOWNLOAD_DELAY": True,
        "DOWNLOAD_TIMEOUT": DOWNLOAD_TIMEOUT,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": GOTO_TIMEOUT_MS,
        "PLAYWRIGHT_LAUNCH_OPTIONS": {
            "args": [
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-gpu",
                "--js-flags=--max-old-space-size=512",
            ],
        },
        "PLAYWRIGHT_CONTEXTS": {
            "default": {
                "user_agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/133.0.0.0 Safari/537.36"
                ),
                "extra_http_headers": {
                    "Accept-Language": "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
                },
            }
        },
        "HTTPERROR_ALLOW_ALL": True,
        "RETRY_ENABLED": False,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category_mapping = self._load_category_mapping()
        self.category_urls = list(self.category_mapping.keys())

        import os as _os
        _root = Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy"))
        csv_path = str(_root / "data" / "secur" / "secur_category.csv")
        self.category_enricher = CategorySpecsEnricher(csv_path, self.supplier_id)

        already_scraped = self._load_already_scraped_urls()
        self.processed_products.update(already_scraped)

    # ──────────────────────────────────────────────────────────────────
    # CATEGORY MAPPING
    # ──────────────────────────────────────────────────────────────────

    def _load_category_mapping(self) -> dict:
        mapping = {}
        import os as _os
        csv_path = (
            Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy"))
            / "data" / "secur" / "secur_category.csv"
        )
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

    # ──────────────────────────────────────────────────────────────────
    # RESUME
    # ──────────────────────────────────────────────────────────────────

    def _load_already_scraped_urls(self) -> set:
        urls: set = set()
        import os as _os
        out_path = (
            Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy"))
            / "data" / "output" / self.output_filename
        )
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
                f"📋 Resume: знайдено {len(urls)} вже спарсених товарів — пропускаємо їх"
            )
        except Exception as e:
            self.logger.error(f"⚠️ Не вдалося завантажити прогрес resume: {e}")
        return urls

    # ──────────────────────────────────────────────────────────────────
    # START
    # ──────────────────────────────────────────────────────────────────

    async def start(self):
        if not self.category_urls:
            return
        first_url = self.category_urls[0]
        self.logger.info(
            f"🚀 СТАРТ ПАРСИНГУ. Перша категорія [1/{len(self.category_urls)}]: {first_url}"
        )
        yield scrapy.Request(
            url=first_url,
            callback=self.parse_category,
            meta=_playwright_meta({
                "category_url":     first_url,
                "category_index":   0,
                "page_number":      1,
                "is_category_page": True,
            }),
            priority=PRIORITY_CATEGORY,
            dont_filter=True,
            errback=self.errback_httpbin,
        )

    # ──────────────────────────────────────────────────────────────────
    # ERRBACK
    # ──────────────────────────────────────────────────────────────────

    def errback_httpbin(self, failure):
        self.logger.error(f"❌ ERRBACK: {failure.value}")
        self.logger.error(f"   URL: {failure.request.url}")

        meta             = failure.request.meta
        category_index   = meta.get("category_index", 0)
        is_category_page = meta.get("is_category_page", False)

        if "TimeoutError" in str(failure.value) or "Timeout" in str(failure.value):
            self.logger.warning(
                f"⏱️  Timeout ({GOTO_TIMEOUT_MS / 1000:.0f}s) — рухаємось далі"
            )

        if is_category_page:
            retry_count = meta.get("_cat_retry", 0)
            if retry_count == 0:
                retry_url = failure.request.url
                self.logger.warning(
                    f"🔄 Retry категорії [{category_index + 1}/{len(self.category_urls)}]"
                )
                yield scrapy.Request(
                    url=retry_url,
                    callback=self.parse_category,
                    meta=_playwright_meta({
                        "category_url":     meta.get("category_url", retry_url),
                        "category_index":   category_index,
                        "page_number":      meta.get("page_number", 1),
                        "is_category_page": True,
                        "_cat_retry":       1,
                    }),
                    priority=PRIORITY_CATEGORY,
                    dont_filter=True,
                    errback=self.errback_httpbin,
                )
            else:
                self.logger.error(f"❌ Retry теж впав, пропускаємо: {failure.request.url}")
                next_cat = self._start_next_category(category_index)
                if next_cat:
                    yield next_cat
        else:
            self.logger.warning(f"⏭️  Пропускаємо товар: {failure.request.url}")

    # ──────────────────────────────────────────────────────────────────
    # PARSE CATEGORY
    # ──────────────────────────────────────────────────────────────────

    def parse_category(self, response):
        category_url   = response.meta["category_url"]
        category_index = response.meta["category_index"]
        page_number    = response.meta.get("page_number", 1)
        category_info  = self.category_mapping.get(category_url, {})

        self.logger.info(
            f"📂 Категорія [{category_index + 1}/{len(self.category_urls)}] "
            f"стор.{page_number}: {response.url}"
        )

        product_links = response.css('div.subCategoryWrap div.productsCardsSlider a::attr(href)').getall()
        new_count = 0

        if not product_links:
            self.logger.warning(f"⚠️ Не знайдено товарів: {response.url}")
        else:
            for i, link in enumerate(product_links):
                product_url = response.urljoin(link)
                if product_url in self.processed_products:
                    continue
                self.processed_products.add(product_url)
                new_count += 1
                yield scrapy.Request(
                    url=product_url,
                    callback=self.parse_product_ua,
                    meta=_playwright_meta({
                        "category_url":     category_url,
                        "category_ru":      category_info.get("category_ru", ""),
                        "category_ua":      category_info.get("category_ua", ""),
                        "group_number":     category_info.get("group_number", ""),
                        "subdivision_id":   category_info.get("subdivision_id", ""),
                        "subdivision_link": category_info.get("subdivision_link", ""),
                    }),
                    priority=PRIORITY_PRODUCT + len(product_links) - i,
                    dont_filter=True,
                    errback=self.errback_httpbin,
                )

            if new_count:
                self.logger.info(f"   ➕ Додано в чергу: {new_count} нових товарів")
            else:
                self.logger.info(f"   ⏭️  Всі товари вже спарсені (Resume)")

        next_page = response.css('a.next-button::attr(href)').get()
        if next_page:
            yield scrapy.Request(
                url=response.urljoin(next_page),
                callback=self.parse_category,
                meta=_playwright_meta({
                    "category_url":     category_url,
                    "category_index":   category_index,
                    "page_number":      page_number + 1,
                    "is_category_page": True,
                }),
                priority=PRIORITY_CATEGORY,
                dont_filter=True,
                errback=self.errback_httpbin,
            )
        else:
            self.logger.info(
                f"✅ ПАГІНАЦІЯ ЗАВЕРШЕНА [{category_index + 1}/{len(self.category_urls)}]"
            )
            next_cat = self._start_next_category(category_index)
            if next_cat:
                yield next_cat

    # ──────────────────────────────────────────────────────────────────
    # PARSE PRODUCT UA
    # ──────────────────────────────────────────────────────────────────

    def parse_product_ua(self, response):
        name_ua   = response.css('h1.title::text').get()
        price_raw = response.css('div.currentPrice span.bold::text').get()

        slider_images = response.css(
            'div.keen-slider__slide img[src*="/images/big/"]::attr(src)'
        ).getall()
        if slider_images:
            seen: set = set()
            unique_images = []
            for url in slider_images:
                if url not in seen:
                    seen.add(url)
                    unique_images.append(url)
            image_url = ', '.join(response.urljoin(u) for u in unique_images)
        else:
            all_big  = response.css('img[src*="/images/big/"]::attr(src)').getall()
            filtered = [u for u in all_big if '/preview/' not in u]
            image_url = ', '.join(response.urljoin(u) for u in filtered)

        product_code        = response.css('div.productsCardsCode span::text').get()
        availability_raw    = (response.css('div.statusWrap::text').get() or "В наявності").strip()
        description_ua_html = response.css('div.content.descr div.item').get()
        description_ua      = self._clean_html_description(description_ua_html) if description_ua_html else ""

        specs_list   = self._parse_specifications(response)
        category_url = response.meta.get("category_url", "")
        if category_url:
            specs_list = self.category_enricher.enrich_specs(specs_list, category_url)

        meta = response.meta.copy()
        meta.update({
            "name_ua":          name_ua.strip() if name_ua else "",
            "price_raw":        price_raw,
            "image_url":        image_url,
            "product_code":     product_code.strip() if product_code else "",
            "availability_raw": availability_raw,
            "description_ua":   description_ua,
            "specs_list":       specs_list,
        })

        ru_url = response.url.replace("secur.ua/", "secur.ua/ru/")
        yield scrapy.Request(
            url=ru_url,
            callback=self.parse_product_ru,
            meta=_playwright_meta(meta),
            priority=PRIORITY_PRODUCT,
            dont_filter=True,
            errback=self.errback_httpbin,
        )

    # ──────────────────────────────────────────────────────────────────
    # PARSE PRODUCT RU
    # ──────────────────────────────────────────────────────────────────

    def parse_product_ru(self, response):
        name_ua = response.meta.get("name_ua", "")

        if response.status != 200:
            self.logger.warning(f"⚠️ RU {response.status}, fallback на UA: {response.url}")
            name_ru        = name_ua
            description_ru = response.meta.get("description_ua", "")
            brand          = ""
        else:
            name_ru_raw = response.css('h1.title::text').get()
            name_ru     = name_ru_raw.strip() if name_ru_raw else name_ua
            brand_raw   = response.xpath(
                "//div[@class='subtitle' and text()='Бренд']"
                "/../div[@class='inner']//p/text()"
            ).get()
            brand               = brand_raw.strip() if brand_raw else ""
            description_ru_html = response.css('div.content.descr div.item').get()
            description_ru      = (
                self._clean_html_description(description_ru_html)
                if description_ru_html else ""
            )

        name_ru = re.sub(r'\s*\([A-Z0-9\.]+\)\s*$', '', name_ru)
        name_ua = re.sub(r'\s*\([A-Z0-9\.]+\)\s*$', '', name_ua)

        price_raw        = response.meta.get("price_raw", "")
        image_url        = response.meta.get("image_url", "")
        product_code     = response.meta.get("product_code", "")
        availability_raw = response.meta.get("availability_raw", "")
        description_ua   = response.meta.get("description_ua", "")
        specs_list       = response.meta.get("specs_list", [])

        price    = self._clean_price(price_raw) if price_raw else ""
        quantity = self._extract_quantity(availability_raw)

        item = {
            "Код_товару":               product_code,
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
            "Наявність":                availability_raw,
            "Кількість":                quantity,
            "Назва_групи":              response.meta.get("category_ru", ""),
            "Назва_групи_укр":          response.meta.get("category_ua", ""),
            "Номер_групи":              response.meta.get("group_number", ""),
            "Ідентифікатор_підрозділу": response.meta.get("subdivision_id", ""),
            "Посилання_підрозділу":     response.meta.get("subdivision_link", ""),
            "Ідентифікатор_товару":     product_code,
            "Виробник":                 brand,
            "Країна_виробник":          "",
            "price_type":               self.price_type,
            "supplier_id":              self.supplier_id,
            "output_file":              self.output_filename,
            "Продукт_на_сайті":         response.url.replace("/ru/", "/"),
            "category_url":             response.meta.get("category_url", ""),
            "specifications_list":      specs_list,
        }

        suffix = " (RU fallback)" if response.status != 200 else ""
        self.logger.info(
            f"✅ YIELD{suffix}: {item['Назва_позиції']} | "
            f"Ціна: {item['Ціна']} | Характеристик: {len(specs_list)}"
        )
        yield item

    # ──────────────────────────────────────────────────────────────────
    # HELPERS
    # ──────────────────────────────────────────────────────────────────

    def _start_next_category(self, current_index: int):
        next_index = current_index + 1
        if next_index >= len(self.category_urls):
            self.logger.info("🎉🎉🎉 ВСІ КАТЕГОРІЇ ОБРОБЛЕНІ 🎉🎉🎉")
            return None
        next_url = self.category_urls[next_index]
        self.logger.info(
            f"🚀 НАСТУПНА КАТЕГОРІЯ [{next_index + 1}/{len(self.category_urls)}]: {next_url}"
        )
        return scrapy.Request(
            url=next_url,
            callback=self.parse_category,
            meta=_playwright_meta({
                "category_url":     next_url,
                "category_index":   next_index,
                "page_number":      1,
                "is_category_page": True,
            }),
            priority=PRIORITY_CATEGORY,
            dont_filter=True,
            errback=self.errback_httpbin,
        )

    def _parse_specifications(self, response) -> list:
        specs_list = []
        items = response.xpath('//div[@class="item"][.//div[@class="subtitle"]]')
        for item in items:
            characteristic = item.xpath('.//div[@class="subtitle"]/text()').get()
            if not characteristic:
                continue
            characteristic = characteristic.strip()
            value_texts = item.xpath('.//div[@class="inner"]//text()').getall()
            value = ' '.join(t.strip() for t in value_texts if t.strip())
            if value:
                specs_list.append({
                    "name":  characteristic,
                    "unit":  "",
                    "value": value.replace('\u00a0', ' ').strip(),
                })
        return specs_list

    def _clean_html_description(self, html_content: str) -> str:
        if not html_content:
            return ""
        description_html = html_content
        description_html = re.sub(r'^<div[^>]*>', '', description_html)
        description_html = re.sub(r'</div>$', '', description_html)
        description_html = re.sub(r'\s*style="[^"]*"', '', description_html)
        description_html = re.sub(r'>\s+<', '><', description_html)
        if len(description_html) > 10_000:
            description_html = description_html[:10_000] + '...</p>'
        return description_html.strip()

    def closed(self, reason):
        self.logger.info(f"🎉 Паук {self.name} завершено! Причина: {reason}")
        if self.failed_products:
            self.logger.info("=" * 80)
            self.logger.info("📦 СПИСОК ТОВАРІВ З ПОМИЛКАМИ ЗАВАНТАЖЕННЯ")
            self.logger.info("=" * 80)
            for failed in self.failed_products:
                self.logger.error(
                    f"- {failed['product_name']} | {failed['url']} | {failed['reason']}"
                )
            self.logger.info("=" * 80)
        else:
            self.logger.info("✅ Товарів з помилками завантаження не знайдено.")
        try:
            import winsound
            for _ in range(3):
                winsound.Beep(1000, 300)
            self.logger.info("🔔 Звуковий сигнал відтворено!")
        except Exception as e:
            self.logger.debug(f"Не вдалося відтворити звук: {e}")
