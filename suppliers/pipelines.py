from __future__ import annotations

import csv
import re
from pathlib import Path
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem


class ManufacturersDB:
    """
    Завантажує {supplier}_manufacturers.csv і надає lookup виробника+країни.

    Формат CSV:
        Слово в названии продукта;Производитель (виробник);Країна_виробник
        Hikvision;Hikvision;Китай
        ...
        ;Без бренду;   ← останній рядок (порожній keyword) — fallback

    Методи:
        lookup(product_name)      → (manufacturer, country) | ("", "")
        lookup_country(mfr_name)  → country str
        no_brand()                → (manufacturer, country) останнього рядку
    """

    def __init__(self, csv_path: Path | None, logger):
        # (ключ_нижній_реєстр, виробник, країна)
        self._entries: list[tuple[str, str, str]] = []
        # зворотний словник виробник(нижній) → країна
        self._mfr_to_country: dict[str, str] = {}
        # fallback — останній рядок з порожнім keyword
        self._no_brand: tuple[str, str] = ("", "")

        if csv_path:
            self._load(csv_path, logger)

    def _load(self, path: Path, logger) -> None:
        entries: list[tuple[str, str, str]] = []
        try:
            with open(path, encoding="utf-8-sig") as f:
                for row in csv.DictReader(f, delimiter=";"):
                    keyword    = row.get("Слово в названии продукта", "").strip()
                    mfr        = row.get("Производитель (виробник)", "").strip()
                    country    = row.get("Країна_виробник", "").strip()

                    if not keyword:
                        # Порожній keyword = fallback "Без бренду" (останній рядок)
                        self._no_brand = (mfr, country)
                    else:
                        entries.append((keyword.lower(), mfr, country))
                        # Зворотний індекс: виробник → країна (перший збіг виграє)
                        self._mfr_to_country.setdefault(mfr.lower(), country)

            # Довгі ключі мають пріоритет — унікаємо "банківських" збігів
            self._entries = sorted(entries, key=lambda x: len(x[0]), reverse=True)
            logger.info(
                f"✅ ManufacturersDB: {len(self._entries)} ключів, "
                f"no-brand='{self._no_brand[0]}' ({path.name})"
            )
        except Exception as exc:
            logger.warning(f"⚠️ ManufacturersDB: не вдалося завантажити {path}: {exc}")

    # ------------------------------------------------------------------ #

    def lookup(self, product_name: str) -> tuple[str, str]:
        """Повертає (manufacturer, country) за назвою товару або ("", "")."""
        if not product_name or not self._entries:
            return ("", "")

        name_lower = product_name.lower()
        for keyword, mfr, country in self._entries:
            if len(keyword) <= 2:
                if re.search(r'\b' + re.escape(keyword) + r'\b', name_lower):
                    return (mfr, country)
            else:
                if keyword in name_lower:
                    return (mfr, country)
        return ("", "")

    def lookup_country(self, manufacturer: str) -> str:
        """Повертає країну за точною назвою виробника (case-insensitive)."""
        return self._mfr_to_country.get(manufacturer.lower(), "")

    def no_brand(self) -> tuple[str, str]:
        """Фаллбек: останній рядок CSV ("Без бренду", "")."""
        return self._no_brand

from suppliers.attribute_mapper import AttributeMapper
from keywords.core.generator import ProductKeywordsGenerator

# Імпортуємо сервіси
from suppliers.services.supplier_config import SupplierConfig
from suppliers.services.channel_service import ChannelService
from suppliers.services.availability_service import AvailabilityService
from suppliers.services.specs_utils import merge_all_specs
from suppliers.services.prom_csv_schema import PromCsvSchema
from suppliers.services.spec_length_handler import SpecificationLengthHandler
from suppliers.services.field_processor import FieldProcessor
from suppliers.services.validation_service import ValidationService
from suppliers.services.sku_code_service import SkuCodeService
from suppliers.services.text_sanitizer import TextSanitizer
from suppliers.constants import get_start_code


class SuppliersPipeline:
    """
    ЄДИНИЙ pipeline для всіх постачальників з підтримкою МУЛЬТИКАНАЛЬНОСТІ.
    
    MULTI-CHANNEL РЕЖИМ:
    - 1 товар від постачальника → N записів (site, prom, rozetka...)
    - Різні ціни, категорії, нотатки для кожного каналу
    - Коефіцієнти цін задані в category.csv
    
    PROM CSV:
    - base поля через PromCsvSchema (єдине джерело правди)
    - 160× (Назва;Одиниця;Значення) БЕЗ нумерації
    """

    SPECS_LIMIT = 160

    # ------------------------------------------------------------------ #
    # INIT
    # ------------------------------------------------------------------ #

    def __init__(self):
        # CSV
        self.files: dict[str, any] = {}
        self.product_counters: dict[str, int] = {}
        self.stats: dict[str, dict] = {}
        self.stats_logged = False

        # Конфігурації
        self.configs: dict[str, SupplierConfig] = {}
        
        # Сервіси (ініціалізуються в open_spider)
        self.channel_services: dict[str, ChannelService] = {}
        self.sku_code_services: dict[str, SkuCodeService] = {}
        self.availability_service = AvailabilityService()
        self.attribute_mapper: AttributeMapper | None = None
        self.keywords_generator: ProductKeywordsGenerator | None = None
        self.spec_length_handler = SpecificationLengthHandler(strategy="hybrid")
        self.field_processor: FieldProcessor | None = None
        self.validation_service = ValidationService()

        # Manufacturers DB: {spider_name: ManufacturersDB}
        # Завантажується з {supplier}_manufacturers.csv для всіх постачальників
        self.manufacturers_db: dict[str, "ManufacturersDB"] = {}

        import os as _os
        self.output_dir = Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy")) / "data" / "output"

    # ------------------------------------------------------------------ #
    # OPEN SPIDER
    # ------------------------------------------------------------------ #

    def open_spider(self, spider):
        """Ініціалізація через SupplierConfig - ZERO magic, ONE source of truth"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Зберігаємо spider logger для використання в _write_row
        self._spider_logger = spider.logger

        # 1️⃣ Створюємо конфігурацію автоматично
        config = SupplierConfig.from_spider(spider.name)
        self.configs[spider.name] = config
        
        spider.logger.info(f"📦 {config}")

        # 2️⃣ Ініціалізуємо сервіси на основі конфігу
        self._init_services(config, spider)

        # 3️⃣ Ініціалізуємо CSV
        # ── RESUME-AWARE ВІДКРИТТЯ ФАЙЛУ ─────────────────────────────
        # Якщо файл вже існує (попередній запуск був перерваний) —
        # відкриваємо в режимі append і НЕ пишемо заголовок повторно.
        # Якщо файлу немає — створюємо новий з заголовком.
        # Це дозволяє Resume коректно накопичувати товари між перезапусками.
        output_file = getattr(spider, "output_filename", f"{spider.name}.csv")
        path = self.output_dir / output_file

        file_exists = path.exists() and path.stat().st_size > 0
        mode = "a" if file_exists else "w"

        self.files[output_file] = open(
            path, mode, encoding="utf-8-sig", newline="", buffering=1
        )

        if not file_exists:
            self._write_header(self.files[output_file])
            spider.logger.info(f"📝 CSV (новий): {path}")
        else:
            spider.logger.info(f"📝 CSV (append/resume): {path}")

        # 4️⃣ Ініціалізуємо SKU→Код сервіс (start_code з constants.py)
        start_code = get_start_code(config.supplier_name)
        sku_map_file = config.data_dir / "sku_map.json"
        self.sku_code_services[spider.name] = SkuCodeService(
            map_file=sku_map_file,
            start_code=start_code,
            logger=spider.logger,
        )

        # 5️⃣ Статистика
        self.stats[output_file] = {
            "count": 0,
            "filtered_no_price": 0,
            "filtered_no_stock": 0,
            "filtered_no_sku": 0,
        }

    def _init_services(self, config: SupplierConfig, spider):
        """Ініціалізація всіх сервісів через конфіг"""
        # Manufacturers DB — завантажуємо завжди, для всіх постачальників
        if config.manufacturers_file and config.manufacturers_file.exists():
            self.manufacturers_db[spider.name] = ManufacturersDB(
                config.manufacturers_file, spider.logger
            )
        else:
            self.manufacturers_db[spider.name] = ManufacturersDB(None, spider.logger)
        
        # ChannelService (NEW - мультиканальний режим)
        if config.use_multi_channel and config.category_file:
            self.channel_services[spider.name] = ChannelService(
                config.category_file,
                spider.logger,
                decimal_places=config.price_decimal_places
            )
            spider.logger.info(f"🔀 Мультиканальний режим активовано для {spider.name}")
        
        # PriceService більше не використовується (LEGACY видалено)
        
        # AttributeMapper
        if config.use_attribute_mapper and config.mapping_rules_file:
            self.attribute_mapper = AttributeMapper(
                str(config.mapping_rules_file), 
                spider.logger
            )
        
        # KeywordsGenerator
        if config.use_keywords_generator and config.keywords_file and config.manufacturers_file:
            self.keywords_generator = ProductKeywordsGenerator(
                str(config.keywords_file),
                str(config.manufacturers_file),
                config.supplier_name,
                spider.logger
            )
        
        # FieldProcessor з category config
        if config.category_file and config.category_file.exists():
            self.field_processor = FieldProcessor(config.category_file)
            spider.logger.info(f"✅ FieldProcessor ініціалізовано з {config.category_file.name}")
        else:
            self.field_processor = FieldProcessor()
            spider.logger.warning(f"⚠️ Category config не знайдено")

    # ------------------------------------------------------------------ #
    # PROCESS ITEM - МУЛЬТИКАНАЛЬНИЙ РЕЖИМ
    # ------------------------------------------------------------------ #

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        output_file = adapter.get("output_file", f"{spider.name}.csv")
        config = self.configs[spider.name]

        # ---- FILTERS (через ValidationService) ----------------------- #

        price = adapter.get("Ціна")
        if not self.validation_service.is_valid_price(price):
            self._inc(output_file, "filtered_no_price")
            raise DropItem("NO PRICE")

        availability_raw = adapter.get("Наявність", "")
        if not self.availability_service.is_available(availability_raw):
            self._inc(output_file, "filtered_no_stock")
            raise DropItem("NO STOCK")

        # ---- MULTI-CHANNEL MODE -------------------------------------- #
        
        channel_service = self.channel_services.get(spider.name)
        
        if channel_service and channel_service.is_multi_channel:
            # 🔀 МУЛЬТИКАНАЛЬНИЙ РЕЖИМ: 1 товар → N записів
            category_url = adapter.get("category_url", "")
            category_id = adapter.get("category_id", "")
            source = adapter.get("source", "site")  # "feed" або "site"
            feed_id = adapter.get("feed_id", "")  # ID фіду для фільтрації каналів

            # фільтруємо канали по feed_id: категорії 25, 13, 621 є в двох фідах —
            # повертаємо тільки канали поточного фіду
            channels = channel_service.resolve_channels(category_url, category_id, feed_id)

            if not channels:
                spider.logger.warning(
                    f"⚠️ Не знайдено каналів для категорії: url={category_url!r}, id={category_id!r}"
                )
                raise DropItem("NO CHANNELS")

            # Зберігаємо базову ціну для множення
            base_price = adapter.get("Ціна")

            # Для кожного каналу створюємо окремий запис
            for channel_config in channels:
                # Клонуємо cleaned item
                cleaned = self._clean_item(adapter, spider)

                # Нормалізована наявність
                avail, qty = self.availability_service.normalize_availability(availability_raw)
                cleaned["Наявність"] = avail
                # Пріоритет: якщо павук дав число — беремо його;
                # None/""/не задано — беремо дефолт з AvailabilityService.
                # ВАЖЛИВО: не використовуємо `or`, бо "0" є falsy і буде замінений дефолтом.
                spider_qty = adapter.get("Кількість")
                cleaned["Кількість"] = spider_qty if spider_qty not in (None, "") else qty

                # ---- CHANNEL-SPECIFIC FIELDS ------------------------- #

                # ✅ ВИПРАВЛЕННЯ 1: для фіду використовуємо coefficient_feed
                coef = (
                    channel_config.coefficient_feed
                    if source == "feed"
                    else channel_config.coefficient
                )
                cleaned["Ціна"] = channel_service.apply_price_coefficient(base_price, coef)
                
                # Код товару - стабільний між запусками, прив'язаний до SKU
                base_sku = adapter.get("Ідентифікатор_товару", "")
                sku_service = self.sku_code_services[spider.name]
                try:
                    product_code = str(sku_service.get_or_create(base_sku))
                except ValueError:
                    spider.logger.warning(
                        f"⚠️ Порожній Ідентифікатор_товару у товару: "
                        f"{adapter.get('Назва_позиції', 'N/A')!r} — пропускаємо"
                    )
                    self._inc(output_file, "filtered_no_sku")
                    raise DropItem("EMPTY SKU")
                cleaned["Код_товару"] = product_code
                
                # Ідентифікатор товару - з префіксом
                if channel_config.prefix:
                    cleaned["Ідентифікатор_товару"] = f"{channel_config.prefix}{base_sku}"
                else:
                    cleaned["Ідентифікатор_товару"] = base_sku
                
                # Категорія, нотатки, ярлик
                cleaned["Номер_групи"] = channel_config.group_number
                cleaned["Назва_групи"] = channel_config.group_name
                cleaned["Ідентифікатор_підрозділу"] = channel_config.subdivision_id
                cleaned["Посилання_підрозділу"] = channel_config.subdivision_link
                cleaned["Особисті_нотатки"] = channel_config.personal_notes
                cleaned["Ярлик"] = channel_config.label
                
                # ---- SPECS ------------------------------------------- #
                
                specs = adapter.get("specifications_list", [])
                
                # 🔪 ОБРОБКА ДОВГИХ ХАРАКТЕРИСТИК
                current_description = cleaned.get("Опис", "")
                specs, updated_description = self.spec_length_handler.process_specifications(
                    specs, current_description
                )
                cleaned["Опис"] = updated_description
                
                specs = self._process_specs(specs, cleaned, adapter, spider)
                
                # ---- POSTPROCESS SPECS ------------------------------- #
                
                category_id = channel_config.subdivision_id
                specs = self.field_processor.process_specs_weight(specs, category_id, spider)
                specs = self.field_processor.process_specs_load_capacity(specs, spider)
                specs = self.field_processor.process_specs_hdd_capacity(specs, spider)
                specs = self.field_processor.process_specs_battery_capacity(specs, spider)
                
                # ---- DIMENSIONS -------------------------------------- #
                
                dimensions = self.field_processor.extract_dimensions_from_specs(specs, spider) or {}
                cleaned.update(dimensions)

                # PROM вимагає крапку (не кому) в базових колонках габаритів.
                # .replace('.', ',') в field_processor потрібен для характеристик —
                # тут конвертуємо назад тільки для цих 4 базових полів.
                for _dim_field in ("Вага,кг", "Ширина,см", "Висота,см", "Довжина,см"):
                    if cleaned.get(_dim_field):
                        cleaned[_dim_field] = cleaned[_dim_field].replace(",", ".")
                
                # ---- KEYWORDS ---------------------------------------- #
                
                if self.keywords_generator:
                    cleaned["Пошукові_запити"] = self.keywords_generator.generate_keywords(
                        cleaned.get("Назва_позиції", ""), category_id, specs, "ru"
                    )
                    cleaned["Пошукові_запити_укр"] = self.keywords_generator.generate_keywords(
                        cleaned.get("Назва_позиції_укр", ""), category_id, specs, "ua"
                    )
                
                # ---- WRITE ------------------------------------------- #
                
                self._write_row(output_file, cleaned, specs)
                self.stats[output_file]["count"] += 1
            
            # Лічильник більше не потрібен — SkuCodeService керує кодами
        else:
            # Якщо немає мультиканального режиму - помилка
            spider.logger.error(f"⚠️ Мультиканальний режим не активовано для {spider.name}")
            raise DropItem("NO MULTI-CHANNEL")

        return item

    # ------------------------------------------------------------------ #
    # CSV
    # ------------------------------------------------------------------ #

    def _write_header(self, f):
        """Генерує CSV заголовок через PromCsvSchema"""
        header = PromCsvSchema.get_header(self.SPECS_LIMIT)
        f.write(";".join(header) + "\n")

    def _write_row(self, output_file, cleaned, specs):
        """Записує рядок у CSV з використанням схеми"""
        row = []

        # Базові поля через схему
        for field in PromCsvSchema.BASE_FIELDS:
            value = cleaned.get(field, "")
            row.append(self.validation_service.sanitize_csv_value(value))

        # Характеристики
        written = 0
        for spec in specs[: self.SPECS_LIMIT]:
            row.extend([
                self.validation_service.sanitize_csv_value(spec.get("name", "")),
                self.validation_service.sanitize_csv_value(spec.get("unit", "")),
                self.validation_service.sanitize_csv_value(
                    self.validation_service.normalize_spec_value(spec.get("value", ""))
                ),
            ])
            written += 1

        # Заповнення порожніх характеристик
        for _ in range(self.SPECS_LIMIT - written):
            row.extend(["", "", ""])

        self.files[output_file].write(";".join(row) + "\n")
        
        # Логування успішного YIELD
        product_name = cleaned.get('Назва_позиції', 'Невідомий')[:60]
        price_display = cleaned.get('Ціна', '0')
        specs_count = len(specs)
        channel = cleaned.get('Особисті_нотатки', 'site')
        
        if hasattr(self, '_spider_logger'):
            self._spider_logger.info(
                f"✅ YIELD [{channel}]: {product_name} | Ціна: {price_display} | Характеристик: {specs_count}"
            )

    # ------------------------------------------------------------------ #
    # CLEAN ITEM (з постпроцесами через FieldProcessor)
    # ------------------------------------------------------------------ #

    def _clean_item(self, adapter, spider):
        """
        Очищає та нормалізує item для CSV через PromCsvSchema.

        ПОСТПРОЦЕСИ:
        - Вага/габарити: конвертація одиниць
        - Виробник + Країна_виробник: заповнюються разом через ManufacturersDB
        """
        result = {}

        for prom_field in PromCsvSchema.BASE_FIELDS:
            v = adapter.get(prom_field, "")

            if not v:
                for item_field, mapped_field in PromCsvSchema.ITEM_TO_PROM_MAPPING.items():
                    if mapped_field == prom_field:
                        v = adapter.get(item_field, "")
                        break

            value = str(v).strip() if v is not None else ""

            if prom_field == "Вага,кг" and value:
                category_id = adapter.get("Ідентифікатор_підрозділу", "")
                value = self.field_processor.process_weight(value, category_id, spider)
            elif prom_field == "Ширина,см" and value:
                value = self.field_processor.process_dimension(value, "Ширина", spider)
            elif prom_field == "Висота,см" and value:
                value = self.field_processor.process_dimension(value, "Висота", spider)
            elif prom_field == "Довжина,см" and value:
                value = self.field_processor.process_dimension(value, "Довжина", spider)
            elif prom_field in ("Назва_позиції_укр", "Опис_укр") and value:
                value = FieldProcessor.normalize_cyrillic(value)

            result[prom_field] = value

        # ---- MANUFACTURER + COUNTRY ---------------------------------- #
        mfr_db = self.manufacturers_db.get(spider.name)
        if mfr_db:
            manufacturer = result.get("Виробник", "")
            country      = result.get("Країна_виробник", "")

            if manufacturer:
                if not country:
                    country = mfr_db.lookup_country(manufacturer)
                if not country:
                    canonical, found_country = mfr_db.lookup(manufacturer)
                    if canonical:
                        manufacturer = canonical
                        country = found_country
            else:
                manufacturer, country = mfr_db.lookup(adapter.get("Назва_позиції", ""))
                if not manufacturer:
                    manufacturer, country = mfr_db.no_brand()

            result["Виробник"]        = manufacturer
            result["Країна_виробник"] = country

        result["Валюта"]         = result.get("Валюта") or "UAH"
        result["Одиниця_виміру"] = result.get("Одиниця_виміру") or "шт."

        # Очищення заборонених слів (Prom.ua)
        TextSanitizer.sanitize_item(result)

        return result

    # ------------------------------------------------------------------ #
    # SPECS
    # ------------------------------------------------------------------ #

    def _process_specs(self, specs, cleaned, adapter, spider):
        """Обробка характеристик через AttributeMapper + merge_all_specs"""
        if not self.attribute_mapper:
            return self._ensure_condition(specs)

        cat = adapter.get("Ідентифікатор_підрозділу", "")
        
        name_specs = self.attribute_mapper.map_product_name(
            cleaned.get("Назва_позиції", ""), cat
        )
        mapped = self.attribute_mapper.map_attributes(specs, cat).get("mapped", [])
        final_specs = merge_all_specs(specs, mapped, name_specs, spider.logger)
        
        return self._ensure_condition(final_specs)

    def _ensure_condition(self, specs: list) -> list:
        """Додає 'Стан: Новий' якщо немає в характеристиках"""
        specs_dict = {s["name"].lower().strip(): s for s in specs}
        if "стан" not in specs_dict:
            specs_dict["стан"] = {"name": "Стан", "unit": "", "value": "Новий"}
        return list(specs_dict.values())

    # ------------------------------------------------------------------ #
    # STATS / CLOSE
    # ------------------------------------------------------------------ #

    def _inc(self, file, key):
        self.stats[file][key] += 1

    def close_spider(self, spider):
        for f in self.files.values():
            f.close()

        # Зберігаємо sku_map на диск
        for sku_service in self.sku_code_services.values():
            sku_service.save()
        
        # Виводимо статистику обробки характеристик
        self.spec_length_handler.print_stats()

        if self.stats_logged:
            return
        self.stats_logged = True

        for file, s in self.stats.items():
            spider.logger.info(
                f"{file}: OK={s['count']} "
                f"NO_PRICE={s['filtered_no_price']} "
                f"NO_STOCK={s['filtered_no_stock']} "
                f"NO_SKU={s['filtered_no_sku']}"
            )
