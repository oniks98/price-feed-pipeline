"""
Spider для парсингу роздрібних цін з e-server.com.ua (UAH)
Вигружає дані в: output/eserver_new.csv

ПОСЛІДОВНА ОБРОБКА: категорія → всі сторінки пагінації → наступна категорія
ХАРАКТЕРИСТИКИ: парсяться УКРАЇНСЬКОЮ (UA) та РОСІЙСЬКОЮ (RU) з окремих URL
ПАГІНАЦІЯ: Підтримка параметрів ?only-inStock та &page=N

РЕФАКТОРИНГ:
- priority замість remaining_products — прибирає зростання пам'яті
  Scrapy сам керує чергою; _skip_product більше не потрібен
"""
import scrapy
import csv
import re
from pathlib import Path
from suppliers.spiders.base import EserverBaseSpider, BaseRetailSpider
from suppliers.items import EserverProductItem
from suppliers.services.category_specs_enricher import CategorySpecsEnricher

PRIORITY_PRODUCT  = 10
PRIORITY_CATEGORY = 0


class EserverRetailSpider(EserverBaseSpider, BaseRetailSpider):
    name = "eserver_retail"
    supplier_id = "eserver"
    output_filename = "eserver_new.csv"

    custom_settings = {
        **EserverBaseSpider.custom_settings,
        "ITEM_PIPELINES": {
            "suppliers.pipelines.SuppliersPipeline": 300,
        },
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.category_mapping  = self._load_category_mapping()
        self.category_urls     = list(self.category_mapping.keys())
        self.keywords_mapping  = self._load_keywords_mapping_eserver()

        import os as _os
        _root = Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy"))
        csv_path = str(_root / "data" / "eserver" / "eserver_category.csv")
        self.category_enricher = CategorySpecsEnricher(csv_path, self.supplier_id)

    # ──────────────────────────────────────────────────────────
    # CATEGORY MAPPING
    # ──────────────────────────────────────────────────────────

    def _load_category_mapping(self):
        mapping = {}
        import os as _os
        csv_path = (
            Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy"))
            / "data" / "eserver" / "eserver_category.csv"
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

    # ──────────────────────────────────────────────────────────
    # START
    # ──────────────────────────────────────────────────────────

    async def start(self):
        if not self.category_urls:
            return
        first = self.category_urls[0]
        self.logger.info(f"🚀 СТАРТ ПАРСИНГУ. Перша категорія [1/{len(self.category_urls)}]: {first}")
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
            f"стор.{page_number}: {response.url}"
        )

        product_links = response.css("div[class*='card'] a[href*='/uk/']::attr(href)").getall()
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
        next_page_link = response.css("li.next a::attr(href)").get()
        if not next_page_link and len(product_links) > 0:
            next_page_link = self._build_next_page_url(category_url, page_number, len(product_links))

        if next_page_link:
            yield response.follow(
                url=next_page_link,
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
    # PARSE PRODUCT (3-шагова ланцюг: switcher → ua → ru)
    # ──────────────────────────────────────────────────────────

    def parse_product(self, response):
        try:
            ua_link = response.css("div.langs_langs__QyR6J a[href*='/uk/']::attr(href)").get()
            ru_link = response.css("div.langs_langs__QyR6J a:not([href*='/uk/'])::attr(href)").get()
            if not ua_link or not ru_link:
                self.logger.error(f"❌ Не знайдено посилань на мови: UA={ua_link}, RU={ru_link}")
                return
            yield scrapy.Request(
                url=response.urljoin(ua_link),
                callback=self.parse_product_ua,
                errback=self.parse_product_error,
                meta={
                    **response.meta,
                    "ru_url":       response.urljoin(ru_link),
                    "original_url": response.url,
                },
                priority=PRIORITY_PRODUCT,
                dont_filter=True,
            )
        except Exception as e:
            self.logger.error(f"❌ Помилка перемикача мов: {response.url} | {e}")

    def parse_product_ua(self, response):
        try:
            name_ua = (response.css("h1.es-h1::text").get() or response.css("h1::text").get() or "").strip()
            description_ua = self._extract_description_from_html(response)
            specs_list_ua  = self._extract_specifications_eserver(response)
            category_url   = response.meta.get("category_url", "")
            if category_url:
                specs_list_ua = self.category_enricher.enrich_specs(specs_list_ua, category_url)
            yield scrapy.Request(
                url=response.meta.get("ru_url"),
                callback=self.parse_product_ru,
                errback=self.parse_product_error,
                meta={
                    **response.meta,
                    "name_ua":             name_ua,
                    "description_ua":      description_ua,
                    "specifications_list": specs_list_ua,
                },
                priority=PRIORITY_PRODUCT,
                dont_filter=True,
            )
        except Exception as e:
            self.logger.error(f"❌ Помилка парсингу (UA): {response.url} | {e}")

    def parse_product_ru(self, response):
        try:
            name_ru = (response.css("h1.es-h1::text").get() or response.css("h1::text").get() or "").strip()
            description_ru = self._extract_description_from_html(response)
            name_ua        = response.meta.get("name_ua", "")
            description_ua = response.meta.get("description_ua", "")
            specs_list     = response.meta.get("specifications_list", [])

            price_raw = response.css("div.flex.items-end.font-bold.text-23px::text").get()
            if not price_raw:
                price_raw = response.css("div[class*='price']::text").get()
            price = self._clean_price(price_raw) if price_raw else ""

            # Наявність
            availability_raw = ""
            avail_el = response.css("div.product_ag-sts__x60QA")
            if avail_el:
                availability_raw = " ".join(
                    t.strip() for t in avail_el.css("::text").getall() if t.strip()
                )
            if not availability_raw:
                for text in response.css("*::text").getall():
                    tl = text.lower().strip()
                    if "наявност" in tl or "налич" in tl:
                        availability_raw = text.strip()
                        break
            if not availability_raw:
                for div in response.css("div[class*='status'], div[class*='stock'], div[class*='available']"):
                    text = " ".join(div.css("::text").getall()).strip()
                    if text:
                        availability_raw = text
                        break
            if not availability_raw:
                availability_raw = "В наявності"

            # Артикул
            sku_texts = response.css("div[data-testid='product-sku']::text").getall()
            sku = ""
            if sku_texts:
                sku_raw = " ".join(t.strip() for t in sku_texts if t.strip())
                match = re.search(
                    r'(?:Артикул\s*:\s*)([A-Za-zА-Яа-яЁёІіЇїЄєҐґ0-9\-_\.\s]+)',
                    sku_raw, re.IGNORECASE
                )
                if match:
                    sku = match.group(1).strip()
                else:
                    sku = sku_raw.replace("Артикул:", "").replace("Артикул", "").replace(":", "").strip()

            image_urls   = self._extract_all_images_from_gallery(response)
            image_url    = ", ".join(image_urls)
            manufacturer = self._extract_manufacturer_from_page(response)
            subdivision_id   = response.meta.get("subdivision_id", "")
            search_terms_ru  = self._generate_search_terms(name_ru, subdivision_id, lang="ru")
            search_terms_ua  = self._generate_search_terms(name_ua, subdivision_id, lang="ua")
            quantity         = self._extract_quantity(availability_raw)

            item = EserverProductItem(
                Код_товару="",
                Назва_позиції=name_ru,
                Назва_позиції_укр=name_ua,
                Пошукові_запити=search_terms_ru,
                Пошукові_запити_укр=search_terms_ua,
                Опис=description_ru,
                Опис_укр=description_ua,
                Тип_товару="r",
                Ціна=price,
                Валюта=self.currency,
                Одиниця_виміру="шт.",
                Посилання_зображення=image_url,
                Наявність=availability_raw,
                Кількість=quantity,
                Назва_групи=response.meta.get("category_ru", ""),
                Назва_групи_укр=response.meta.get("category_ua", ""),
                Номер_групи=response.meta.get("group_number", ""),
                Ідентифікатор_підрозділу=response.meta.get("subdivision_id", ""),
                Посилання_підрозділу=response.meta.get("subdivision_link", ""),
                Ідентифікатор_товару=sku,
                Виробник=manufacturer,
                Країна_виробник="",
                price_type=self.price_type,
                supplier_id=self.supplier_id,
                output_file=self.output_filename,
                Продукт_на_сайті=response.meta.get("original_url", response.url),
                category_url=response.meta.get("category_url", ""),
                specifications_list=specs_list,
            )
            self.logger.info(
                f"✅ YIELD: {item['Назва_позиції']} | "
                f"Ціна: {item['Ціна']} | Характеристик: {len(specs_list)}"
            )
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
            self.logger.info("🎉🎉🎉 ВСІ КАТЕГОРІЇ ТА ПРОДУКТИ ОБРОБЛЕНІ 🎉🎉🎉")
            return None
        next_url = self.category_urls[next_index]
        self.logger.info(
            f"🚀 НАСТУПНА КАТЕГОРІЯ [{next_index + 1}/{len(self.category_urls)}]: {next_url}"
        )
        return scrapy.Request(
            url=next_url,
            callback=self.parse_category,
            meta={"category_url": next_url, "category_index": next_index, "page_number": 1},
            priority=PRIORITY_CATEGORY,
            dont_filter=True,
        )

    def _build_next_page_url(self, category_url, current_page, products_count):
        if products_count == 0:
            return None
        next_page = current_page + 1
        if '/page/' in category_url:
            return re.sub(r'/page/\d+', f'/page/{next_page}', category_url)
        return f"{category_url.rstrip('/')}/page/{next_page}"

    def _extract_manufacturer_from_page(self, response):
        try:
            for xpath in [
                "//div[contains(text(), 'Виробник')]",
                "//div[contains(text(), 'Производитель')]",
            ]:
                divs = response.xpath(xpath)
                if divs:
                    mfr = divs[0].css("a::text").get()
                    if mfr:
                        return mfr.strip().replace("™", "").strip()
        except Exception as e:
            self.logger.warning(f"⚠️ Помилка парсингу виробника: {e}")
        return ""

    def _extract_all_images_from_gallery(self, response):
        image_urls = []
        for slide in response.css("div.swiper-slide img[srcset]"):
            srcset = slide.css("::attr(srcset)").get()
            if srcset:
                urls = re.findall(r'(https?://[^\s]+)\s+\d+w', srcset)
                if urls:
                    sanitized = self._sanitize_image_url(urls[-1].rstrip(','))
                    if sanitized and sanitized not in image_urls:
                        image_urls.append(sanitized)
        if not image_urls:
            src = response.css("img[alt*='фото']::attr(src)").get()
            if src:
                sanitized = self._sanitize_image_url(response.urljoin(src))
                if sanitized:
                    image_urls.append(sanitized)
        return image_urls

    def _extract_specifications_eserver(self, response):
        specs = []
        spec_container = response.css("div.bg-white")
        if not spec_container:
            return specs
        for row in spec_container.css("div.flex.justify-between.mx-3"):
            name_el = row.css("div.font-semibold::text").get()
            name = name_el.strip() if name_el else ""
            value_els = row.css("div.text-right::text, div.whitespace-pre-line::text").getall()
            if not value_els:
                value_els = row.css("div.font-medium::text").getall()
            value = "<br>".join(v.strip() for v in value_els if v.strip())
            if name and value:
                specs.append({"name": name, "unit": "", "value": value})
        return specs

    def _extract_description_from_html(self, response):
        container = response.css("div.product_pg-dsc__h3fai")
        if not container:
            return ""
        paragraphs = container.css("p::text").getall()
        if paragraphs:
            return "\n".join(p.strip() for p in paragraphs if p.strip())
        return " ".join(t.strip() for t in container.css("::text").getall() if t.strip())

    def _load_keywords_mapping_eserver(self):
        mapping = {}
        import os as _os
        csv_path = (
            Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy"))
            / "data" / "eserver" / "eserver_keywords.csv"
        )
        if not csv_path.exists():
            self.logger.warning("eserver_keywords.csv not found")
            return mapping
        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    sid = row["Ідентифікатор_підрозділу"].strip()
                    u_ru = [p.strip() for p in row.get("universal_phrases_ru", "").strip().strip('"').split(",") if p.strip()]
                    u_ua = [p.strip() for p in row.get("universal_phrases_ua", "").strip().strip('"').split(",") if p.strip()]
                    b_ru = row.get("base_keyword_ru", "").strip()
                    b_ua = row.get("base_keyword_ua", "").strip()
                    allowed = [s.strip() for s in row.get("allowed_specs", "").strip().split(",") if s.strip()]
                    if sid not in mapping:
                        mapping[sid] = {"universal_phrases_ru": [], "universal_phrases_ua": [],
                                        "base_keyword_ru": "", "base_keyword_ua": "", "allowed_specs": []}
                    mapping[sid]["universal_phrases_ru"].extend(u_ru)
                    mapping[sid]["universal_phrases_ua"].extend(u_ua)
                    if not mapping[sid]["base_keyword_ru"] and b_ru:
                        mapping[sid]["base_keyword_ru"] = b_ru
                    if not mapping[sid]["base_keyword_ua"] and b_ua:
                        mapping[sid]["base_keyword_ua"] = b_ua
                    mapping[sid]["allowed_specs"].extend(allowed)
            for sid in mapping:
                mapping[sid]["universal_phrases_ru"] = list(set(mapping[sid]["universal_phrases_ru"]))
                mapping[sid]["universal_phrases_ua"] = list(set(mapping[sid]["universal_phrases_ua"]))
                mapping[sid]["allowed_specs"]        = list(set(mapping[sid]["allowed_specs"]))
            self.logger.info(f"✅ Завантажено {len(mapping)} підрозділів з ключовими словами")
        except Exception as e:
            self.logger.error(f"❌ Помилка завантаження eserver_keywords.csv: {e}")
        return mapping

    def _generate_search_terms(self, title: str, subdivision_id: str = "", lang: str = "ua") -> str:
        if not title:
            return ""
        components = self._extract_model_components(title, lang)
        if subdivision_id and subdivision_id in self.keywords_mapping:
            phrases_key = f"universal_phrases_{lang}"
            base_key    = f"base_keyword_{lang}"
            components.extend(self.keywords_mapping[subdivision_id].get(phrases_key, [])[:10])
            base = self.keywords_mapping[subdivision_id].get(base_key, "")
            if base:
                components.append(base)
        seen, unique = set(), []
        for term in components:
            tl = term.lower()
            if tl not in seen:
                unique.append(term)
                seen.add(tl)
        return ", ".join(unique[:20])
