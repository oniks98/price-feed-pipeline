"""
Spider для обробки XML-фідів від Secur.
Вигружує дані в: data/output/secur_new.csv (той самий файл що і retail.py)

ЛОГІКА ОДНОГО ЗАПУСКУ:
- Всі фіди (50, 52, 54) обробляються послідовно автоматично
- Для кожного фіду: UA фаза (кеш назв) → RU фаза (yield items)
- pipeline накидає всі товари з усіх фідів в один файл

ВИПРАВЛЕНО:
1. Ціна береться з <dealerPrice>, множиться на coefficient_feed (в pipeline)
2. Виробник береться з <brand>
3. Характеристики збагачуються через CategorySpecsEnricher за category id
4. Категорії без URL (напр. id=282) обробляються через category_id fallback
5. Категорія 739 (Уцінка) видаляється (subdivision_id = "delete")
"""
import scrapy
import csv
import re
from pathlib import Path
from scrapy.selector import Selector

from suppliers.services.category_specs_enricher import CategorySpecsEnricher


class SecurFeedSpider(scrapy.Spider):
    """
    Ajax (крупний опт)
    https://secur.ua/feed/export/50
    https://secur.ua/feed/export/50?lang=ru

    Імпорт (крупний опт)
    https://secur.ua/feed/export/52
    https://secur.ua/feed/export/52?lang=ru

    Україна (крупний опт)
    https://secur.ua/feed/export/54
    https://secur.ua/feed/export/54?lang=ru
    """

    name = "secur_feed"
    supplier_id = "secur"
    allowed_domains = ["secur.ua"]

    # Всі фіди обробляються послідовно в одному запуску
    # 50 = Ajax (крупний опт), 52 = Імпорт (крупний опт), 54 = Україна (крупний опт)
    ALL_FEED_IDS = ["50", "52", "54"]

    custom_settings = {
        "ITEM_PIPELINES": {
            "suppliers.pipelines.SuppliersPipeline": 300,
        },
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_filename = "secur_new.csv"
        self.currency = "UAH"
        self.price_type = "retail"

        self.category_mapping = self._load_category_mapping()

        import os as _os
        _root = Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy"))
        csv_path = str(_root / "data" / "secur" / "secur_category.csv")
        self.category_enricher = CategorySpecsEnricher(csv_path, self.supplier_id)

        # Кеш UA-версій — очищається перед кожним новим фідом
        self.products_ua: dict = {}

    # ------------------------------------------------------------------
    # LOADING
    # ------------------------------------------------------------------

    def _load_category_mapping(self) -> dict:
        mapping: dict = {}
        import os as _os
        csv_path = Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy")) / "data" / "secur" / "secur_category.csv"

        try:
            with open(csv_path, encoding="utf-8-sig") as f:
                reader = csv.DictReader(f, delimiter=";")
                for row in reader:
                    if row.get("channel", "").strip() != "site":
                        continue
                    category_id = row.get("category id", "").strip()
                    if not category_id or category_id in mapping:
                        continue
                    # feed: конкретний фід або порожньо (= всі фіди)
                    allowed_feed = row.get("feed", "").strip()

                    mapping[category_id] = {
                        "category_name": row.get("Назва_групи", ""),
                        "group_number": row.get("Номер_групи", ""),
                        "subdivision_id": row.get("Ідентифікатор_підрозділу", "").strip(),
                        "subdivision_link": row.get("Посилання_підрозділу", ""),
                        "category_url": row.get("Линк категории поставщика", "").strip().strip('"'),
                        "feed": allowed_feed,  # "" = всі фіди, "50" = тільки фід 50
                    }
            self.logger.info(f"✅ Завантажено {len(mapping)} category mappings (feed)")
        except Exception as e:
            self.logger.error(f"❌ Помилка завантаження category mappings: {e}")

        return mapping

    def _is_deleted_category(self, category_id: str) -> bool:
        """True якщо категорія позначена як 'delete' (наприклад Уцінка id=739)."""
        return self.category_mapping.get(category_id, {}).get("subdivision_id", "").lower() == "delete"

    def _is_allowed_for_feed(self, category_id: str, feed_id: str) -> bool:
        """
        True якщо категорія дозволена для цього фіду.
        - feed == ""  → працює для всіх фідів
        - feed == "50" → тільки для фіду 50
        """
        allowed_feed = self.category_mapping.get(category_id, {}).get("feed", "")
        return allowed_feed == "" or allowed_feed == str(feed_id)

    # ------------------------------------------------------------------
    # CRAWLING — послідовна обробка всіх фідів
    # ------------------------------------------------------------------

    async def start(self):
        """Запускає UA-фазу для першого фіду. Решта запускаються ланцюжком."""
        self.logger.info(
            f"🚀 Починаю обробку {len(self.ALL_FEED_IDS)} фідів: {self.ALL_FEED_IDS}"
        )
        yield self._ua_request(feed_index=0)

    def _ua_request(self, feed_index: int) -> scrapy.Request:
        """Формує запит на UA-версію фіду за індексом."""
        fid = self.ALL_FEED_IDS[feed_index]
        return scrapy.Request(
            url=f"https://secur.ua/feed/export/{fid}",
            callback=self.parse_ua_feed,
            meta={"feed_id": fid, "feed_index": feed_index},
            dont_filter=True,
        )

    def parse_ua_feed(self, response):
        """
        Парсить UA-версію фіду — кешує назви та описи.
        Потім запускає RU-фазу для того самого фіду.
        """
        feed_id = response.meta["feed_id"]
        feed_index = response.meta["feed_index"]

        selector = Selector(text=response.text, type="xml")
        selector.remove_namespaces()
        self.logger.info(
            f"📂 [{feed_index + 1}/{len(self.ALL_FEED_IDS)}] UA фід {feed_id} ..."
        )

        # Очищаємо кеш перед кожним новим фідом
        self.products_ua = {}

        for offer in selector.xpath("//offer"):
            product_id = offer.xpath("@id").get()
            if not product_id:
                continue

            category_id = offer.xpath("categoryId/text()").get() or ""
            if self._is_deleted_category(category_id):
                continue
            if not self._is_allowed_for_feed(category_id, feed_id):
                continue

            name_ua = offer.xpath("name/text()").get() or ""
            name_ua = re.sub(r'\s*\([A-Z0-9\.]+\)\s*$', '', name_ua).strip()

            self.products_ua[product_id] = {
                "name_ua": name_ua,
                "description_ua": self._clean_description(
                    offer.xpath("description/text()").get()
                ),
            }

        self.logger.info(
            f"✅ UA фід {feed_id}: {len(self.products_ua)} товарів"
        )

        # Запускаємо RU-фазу для цього самого фіду
        yield scrapy.Request(
            url=f"https://secur.ua/feed/export/{feed_id}?lang=ru",
            callback=self.parse_ru_feed,
            meta={"feed_id": feed_id, "feed_index": feed_index},
            dont_filter=True,
        )

    def parse_ru_feed(self, response):
        """
        Парсить RU-версію фіду — об'єднує з UA кешем, yield items.
        Після завершення запускає UA-фазу наступного фіду (якщо є).
        """
        feed_id = response.meta["feed_id"]
        feed_index = response.meta["feed_index"]

        selector = Selector(text=response.text, type="xml")
        selector.remove_namespaces()
        self.logger.info(
            f"📂 [{feed_index + 1}/{len(self.ALL_FEED_IDS)}] RU фід {feed_id} ..."
        )

        total = mapped = deleted = 0
        unmapped_categories: set = set()

        for offer in selector.xpath("//offer"):
            product_id = offer.xpath("@id").get()
            category_id = offer.xpath("categoryId/text()").get() or ""

            if self._is_deleted_category(category_id):
                deleted += 1
                continue
            if not self._is_allowed_for_feed(category_id, feed_id):
                continue

            total += 1

            name_ru = offer.xpath("name/text()").get() or ""
            name_ru = re.sub(r'\s*\([A-Z0-9\.]+\)\s*$', '', name_ru).strip()
            description_ru = self._clean_description(offer.xpath("description/text()").get())

            ua_data = self.products_ua.get(product_id, {})

            if category_id in self.category_mapping:
                mapped += 1
            else:
                unmapped_categories.add(category_id)

            item = self._build_item(
                offer=offer,
                name_ru=name_ru,
                name_ua=ua_data.get("name_ua", name_ru),
                description_ru=description_ru,
                description_ua=ua_data.get("description_ua", description_ru),
                category_id=category_id,
            )

            if item:
                yield item

        self.logger.info(
            f"✅ Фід {feed_id}: {total} товарів, {deleted} видалено (уцінка), {mapped} з маппінгом"
        )
        if unmapped_categories:
            self.logger.warning(
                f"⚠️ Фід {feed_id} — unmapped categories: {sorted(unmapped_categories)}"
            )

        # Запускаємо наступний фід, якщо є
        next_index = feed_index + 1
        if next_index < len(self.ALL_FEED_IDS):
            self.logger.info(
                f"➡️  Перехід до фіду [{next_index + 1}/{len(self.ALL_FEED_IDS)}]: "
                f"{self.ALL_FEED_IDS[next_index]}"
            )
            yield self._ua_request(feed_index=next_index)
        else:
            self.logger.info("🎉 Всі фіди оброблені!")

    # ------------------------------------------------------------------
    # ITEM BUILDING
    # ------------------------------------------------------------------

    def _build_item(
        self,
        offer,
        name_ru: str,
        name_ua: str,
        description_ru: str,
        description_ua: str,
        category_id: str,
    ) -> dict | None:
        product_id = offer.xpath("@id").get()

        # dealerPrice — оптова ціна; fallback на price
        price_raw = self._to_float(offer.xpath("dealerPrice/text()").get())
        if price_raw is None:
            price_raw = self._to_float(offer.xpath("price/text()").get())

        if not price_raw:
            return None

        url = offer.xpath("url/text()").get() or ""
        available = offer.xpath("@available").get() == "true"
        vendor_code = offer.xpath("vendorCode/text()").get() or ""

        brand = (
            offer.xpath("brand/text()").get()
            or offer.xpath("vendor/text()").get()
            or ""
        ).strip()

        pictures = offer.xpath("picture/text()").getall()
        image_url = ", ".join(pictures) if pictures else ""

        availability_raw = "В наявності" if available else "Немає в наявності"
        quantity = "0"  # якщо available=False; якщо True — pipeline підставить дефолт з AvailabilityService

        category_info = self.category_mapping.get(category_id, {})

        specs_list: list = []
        specs_list = self.category_enricher.enrich_specs_by_category_id(specs_list, category_id)

        return {
            "supplier_id": self.supplier_id,
            "output_file": self.output_filename,
            "price_type": self.price_type,
            "source": "feed",  # pipeline використає coefficient_feed

            "Код_товару": vendor_code or product_id,
            "Ідентифікатор_товару": product_id,

            "Назва_позиції": name_ru,
            "Назва_позиції_укр": name_ua,
            "Опис": description_ru,
            "Опис_укр": description_ua,

            "Тип_товару": "r",
            "Ціна": str(price_raw),  # RAW — pipeline помножить на coefficient_feed
            "Валюта": self.currency,
            "Одиниця_виміру": "шт.",

            "Посилання_зображення": image_url,
            "Наявність": availability_raw,
            "Кількість": quantity,

            "Виробник": brand,
            "Країна_виробник": "",
            "Продукт_на_сайті": url,

            "Пошукові_запити": "",
            "Пошукові_запити_укр": "",

            "Назва_групи": category_info.get("category_name", ""),
            "Назва_групи_укр": category_info.get("category_name", ""),
            "Номер_групи": category_info.get("group_number", ""),
            "Ідентифікатор_підрозділу": category_info.get("subdivision_id", ""),
            "Посилання_підрозділу": category_info.get("subdivision_link", ""),

            # Обидва ключі для channel lookup (pipeline: URL → якщо порожній → category_id)
            "category_url": category_info.get("category_url", ""),
            "category_id": category_id,

            "specifications_list": specs_list,
        }

    # ------------------------------------------------------------------
    # UTILS
    # ------------------------------------------------------------------

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
        description = re.sub(r'<!\[CDATA\[', '', description)
        description = re.sub(r'\]\]>', '', description)
        description = re.sub(r'\s*style="[^"]*"', '', description)
        description = re.sub(r'>\s+<', '><', description)
        if len(description) > 10000:
            description = description[:10000] + '...</p>'
        return description.strip()
