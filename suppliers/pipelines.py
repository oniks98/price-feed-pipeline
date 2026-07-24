from __future__ import annotations

import csv
import hashlib
import re
from decimal import Decimal
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
from suppliers.services.dealer_price_service import DealerPriceService
from suppliers.services.channel_service import ChannelService
from suppliers.services.availability_service import AvailabilityService
from suppliers.services.specs_utils import merge_all_specs
from suppliers.services.prom_csv_schema import PromCsvSchema
from suppliers.services.specs_enricher import SpecsEnricher
from suppliers.services.spec_length_handler import SpecificationLengthHandler
from suppliers.services.spec_limit_handler import SpecLimitService, PROM_HARD_LIMIT, PROM_CSV_SPECS_LIMIT
from suppliers.services.required_guarantee import RequiredGuaranteeService
from suppliers.services.field_processor import FieldProcessor
from suppliers.services.validation_service import ValidationService
from suppliers.services.sku_code_service import SkuCodeService
from suppliers.services.text_sanitizer import TextSanitizer
from suppliers.services.image_service import ImageService
from suppliers.services.stock_fallback import resolve_fallback_qty
from suppliers.constants import get_start_code

RAW_CSV_ROWS_FIELD = "__raw_csv_rows__"


def _compute_config_hash(paths: list) -> str:
    """
    MD5 всіх існуючих конфіг-файлів постачальника (sorted by name).
    Відсутні/None файли пропускаються без помилок.
    """
    hasher = hashlib.md5(usedforsecurity=False)
    for path in sorted(
        (p for p in paths if p is not None and Path(p).exists()),
        key=lambda p: Path(p).name,
    ):
        hasher.update(Path(path).read_bytes())
    return hasher.hexdigest()


def _check_config_invalidation(
    config: "SupplierConfig",
    sku_service: "SkuCodeService",
    spider,
) -> None:
    """
    Порівнює MD5-хеш конфіг-файлів постачальника з попереднім run.
    Якщо хеш змінився (будь-який з файлів) — скидає fast-path:
    old_index та old_headers очищаються, всі товари підуть через
    повний перепарсинг і отримають оновлені хар-ки / групи / ключові слова.

    Відстежувані файли:
        {supplier}_category.csv      → CategorySpecsEnricher, групи, канали
        {supplier}_keywords.csv      → ProductKeywordsGenerator
        {supplier}_manufacturers.csv → ManufacturersDB (виробники, країни)
        {supplier}_mapping_rules.csv → AttributeMapper (маппінг хар-к)
    """
    current_hash = _compute_config_hash([
        config.category_file,
        config.keywords_file,
        config.manufacturers_file,
        config.mapping_rules_file,
    ])
    stored_hash = sku_service.get_meta("config_hash")

    if stored_hash == current_hash:
        return

    if stored_hash:
        spider.logger.info(
            "♻️  Config-файли змінились → fast-path скинуто, "
            "всі товари пройдуть повний перепарсинг"
        )
        if hasattr(spider, "old_index"):
            spider.old_index = {}
        if hasattr(spider, "old_headers"):
            spider.old_headers = []
    else:
        spider.logger.info(f"🔑 Config hash збережено (перший run)")

    sku_service.set_meta("config_hash", current_hash)


class SuppliersPipeline:
    """
    ЄДИНИЙ pipeline для всіх постачальників з підтримкою МУЛЬТИКАНАЛЬНОСТІ.
    
    MULTI-CHANNEL РЕЖИМ:
    - 1 товар від постачальника → N записів (site, prom, rozetka...)
    - Різні ціни, категорії, нотатки для кожного каналу
    - Коефіцієнти цін задані в category.csv
    
    PROM CSV:
    - base поля через PromCsvSchema (єдине джерело правди)
    - 101× (Назва;Одиниця;Значення) БЕЗ нумерації (ліміт Prom.ua = 100)
    """

    # (без локальної SPECS_LIMIT — використовується PROM_CSV_SPECS_LIMIT з spec_limit_handler)

    # ------------------------------------------------------------------ #
    # INIT
    # ------------------------------------------------------------------ #

    def __init__(self):
        # CSV
        self.files: dict[str, any] = {}
        self.product_counters: dict[str, int] = {}
        self.stats: dict[str, dict] = {}
        self.stats_logged = False

        # Anomaly price log (ціна > роздріб постачальника)
        self._anomaly_log = None

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
        self.image_services: dict[str, ImageService] = {}  # per-supplier, як sku_code_services

        # Manufacturers DB: {spider_name: ManufacturersDB}
        # Завантажується з {supplier}_manufacturers.csv для всіх постачальників
        self.manufacturers_db: dict[str, "ManufacturersDB"] = {}

        import os as _os
        self.output_dir = Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline")) / "data" / "output"
        _anomaly_path = Path(_os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline")) / "anomal_price.log"
        self._anomaly_log = open(_anomaly_path, "a", encoding="utf-8", buffering=1)

    # ------------------------------------------------------------------ #
    # OPEN SPIDER
    # ------------------------------------------------------------------ #

    def open_spider(self, spider):
        """Ініціалізація через SupplierConfig - ZERO magic, ONE source of truth"""
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Зберігаємо spider logger для використання в _write_row
        self._spider_logger = spider.logger

        # Глушимо scrapy.core.scraper WARNING — він дампає повний item при DropItem.
        # Натомість логуємо коротко самі (нижче, перед кожним raise DropItem).
        import logging
        logging.getLogger("scrapy.core.scraper").setLevel(logging.ERROR)

        # 1️⃣ Створюємо конфігурацію автоматично
        config = SupplierConfig.from_spider(spider.name)
        self.configs[spider.name] = config
        
        spider.logger.info(f"📦 {config}")

        # 2️⃣ Ініціалізуємо сервіси на основі конфігу (включно з ImageService)
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

        # 5️⃣ Перевірка config-хешу — скидає fast-path якщо змінились конфіги
        _check_config_invalidation(
            config,
            self.sku_code_services[spider.name],
            spider,
        )

        # 6️⃣ Статистика
        self.stats[output_file] = {
            "count": 0,
            "filtered_no_price": 0,
            "filtered_no_stock": 0,
            "filtered_no_sku": 0,
            "guarantee_defaults": {},
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

        # ImageService — per-supplier, кеш в data/{supplier}/image_cache.json
        cache_path = config.data_dir / "image_cache.json"
        self.image_services[spider.name] = ImageService(
            cache_path=cache_path,
            logger=spider.logger,
        )
        spider.logger.info(f"🖼️  ImageService ({spider.name}): кеш → {cache_path}")

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

        raw_rows = adapter.get(RAW_CSV_ROWS_FIELD)
        if raw_rows is not None:
            svc = self.image_services.get(spider.name)
            try:
                img_col = list(PromCsvSchema.BASE_FIELDS).index("Посилання_зображення")
            except ValueError:
                img_col = -1
            written = 0
            for row in raw_rows:
                # При fast-path трансформуємо URL зображення безпосередньо в рядку.
                # wsrv.nl URL проходить через resolve_url без змін (захист у _resolve_single).
                if svc and img_col >= 0 and img_col < len(row) and row[img_col]:
                    row = list(row)
                    row[img_col] = svc.resolve_url(row[img_col])
                self._write_raw_row(output_file, row)
                written += 1
            self.stats[output_file]["count"] += written
            if written and hasattr(self, "_spider_logger"):
                self._spider_logger.info(f"⚡ FAST CSV ROWS: записано {written}")
            return item

        config = self.configs[spider.name]

        # ---- FILTERS (через ValidationService) ----------------------- #

        price = adapter.get("Ціна")
        if not self.validation_service.is_valid_price(price):
            self._inc(output_file, "filtered_no_price")
            spider.logger.warning(
                f"⏭️  DROP NO_PRICE: {adapter.get('Назва_позиції', '?')[:60]} "
                f"| {adapter.get('Продукт_на_сайті', '')}"
            )
            raise DropItem("NO PRICE")

        availability_raw = adapter.get("Наявність", "")
        if not self.availability_service.is_available(availability_raw):
            self._inc(output_file, "filtered_no_stock")
            spider.logger.warning(
                f"⏭️  DROP NO_STOCK: {adapter.get('Назва_позиції', '?')[:60]} "
                f"| {adapter.get('Продукт_на_сайті', '')}"
            )
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

                # ── ЦІНА + ВАЛЮТА + ОПТОВА_ЦІНА ────────────────────────────
                # Viatec dealer (usd_rate є в item):
                #   dealer_uah = dealer_usd × usd_rate  → Оптова_ціна
                #   X = retail / dealer * coef
                #   Ціна = max(retail * coef, dealer * threshold)


                # Secur dealer (dealer_price_uah є в item, вже в UAH):
                #   та сама формула, coef/threshold з CSV
                # Legacy / інші пауки (без жодного з вище): коефіцієнтний режим
                usd_rate_raw         = adapter.get("usd_rate", "")
                dealer_price_uah_raw = adapter.get("dealer_price_uah", "")
                price_rrp_uah        = adapter.get("price_rrp_uah", "")

                # Трекінг для anomaly-логу
                _price_decimal: Decimal = Decimal("0")
                _retail_for_anomaly: Decimal = Decimal("0")
                supplier_retail_uah = DealerPriceService.to_decimal(price_rrp_uah, Decimal("0"))
                if supplier_retail_uah > 0:
                    cleaned["Мінімальний_обсяг_замовлення"] = DealerPriceService.format_price(
                        supplier_retail_uah
                    )

                if usd_rate_raw:
                    # ── Viatec: USD → UAH конвертація ──
                    dealer_uah = DealerPriceService.dealer_uah(base_price, usd_rate_raw)
                    cleaned["Оптова_ціна"] = DealerPriceService.format_price(dealer_uah)
                    _price_decimal = DealerPriceService.channel_price_for_config(
                        channel_config=channel_config,
                        retail_uah=price_rrp_uah,
                        dealer_uah_val=dealer_uah,
                        logger=spider.logger,
                        product_name=adapter.get("Назва_позиції", ""),
                    )
                    cleaned["Ціна"]   = DealerPriceService.format_price(_price_decimal)
                    cleaned["Валюта"] = "UAH"
                    _retail_for_anomaly = supplier_retail_uah

                elif dealer_price_uah_raw:
                    # ── Secur: вже в UAH ──
                    dealer_uah = DealerPriceService.to_decimal(
                        dealer_price_uah_raw, Decimal("0")
                    )
                    cleaned["Оптова_ціна"] = DealerPriceService.format_price(dealer_uah)
                    _price_decimal = DealerPriceService.channel_price_for_config(
                        channel_config=channel_config,
                        retail_uah=price_rrp_uah,
                        dealer_uah_val=dealer_uah,
                        logger=spider.logger,
                        product_name=adapter.get("Назва_позиції", ""),
                    )
                    cleaned["Ціна"]   = DealerPriceService.format_price(_price_decimal)
                    cleaned["Валюта"] = "UAH"
                    _retail_for_anomaly = supplier_retail_uah
                else:
                    # ── Legacy: коефіцієнтний режим (не-dealer або інші постачальники) ──
                    coef = (
                        channel_config.coefficient_feed
                        if source == "feed"
                        else channel_config.coefficient
                    )
                    if channel_config.channel == "prom":
                        price_rrp_uah = adapter.get("price_rrp_uah", "")
                        if price_rrp_uah:
                            cleaned["Ціна"]   = channel_service.apply_price_coefficient(price_rrp_uah, coef)
                            cleaned["Валюта"] = "UAH"
                        else:
                            spider.logger.warning(
                                f"⚠️ prom: відсутня РРЦ UAH, fallback USD | "
                                f"{adapter.get('Назва_позиції', '?')[:60]}"
                            )
                            cleaned["Ціна"] = channel_service.apply_price_coefficient(base_price, coef)
                    else:
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

                # ── Аномальна ціна: розрахована > роздріб постачальника ──
                if _retail_for_anomaly > 0 and _price_decimal > _retail_for_anomaly:
                    self._log_anomaly(
                        product_code=product_code,
                        retail=_retail_for_anomaly,
                        price=_price_decimal,
                    )
                
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

                # ---- FALLBACK QTY (тільки канал "site" — саме з нього Rozetka/
                # Каста/Епіцентр збирають фіди; "prom" свідомо не займаємо) ---- #
                if channel_config.channel == "site":
                    fallback_qty, fallback_reason = resolve_fallback_qty(
                        item_id=cleaned["Код_товару"],
                        subdivision_id=cleaned["Ідентифікатор_підрозділу"],
                        price=cleaned["Ціна"],
                        qty=cleaned["Кількість"],
                    )
                    cleaned["Кількість"] = fallback_qty
                    if fallback_reason == "applied":
                        spider.logger.info(
                            f"🎲 FALLBACK QTY: {cleaned.get('Назва_позиції', '?')[:50]} | "
                            f"підрозділ={cleaned['Ідентифікатор_підрозділу']} "
                            f"ціна={cleaned['Ціна']} → qty={fallback_qty}"
                        )
                    elif fallback_reason in ("invalid_price", "no_band_match"):
                        spider.logger.warning(
                            f"⚠️ FALLBACK QTY [{fallback_reason}]: "
                            f"{cleaned.get('Назва_позиції', '?')[:50]} | "
                            f"підрозділ={cleaned['Ідентифікатор_підрозділу']} "
                            f"ціна={cleaned['Ціна']!r}"
                        )
                
                # ---- SPECS ------------------------------------------- #

                specs = adapter.get("specifications_list", [])

                # Інжектуємо віртуальні характеристики з channel_config у самий
                # початок specs. Для LP API: додає "Тип устройства" зі значенням
                # з lp_category.csv (колонки Назва_Характеристики / Значення_Характеристики).
                # Це дозволяє характеристиці з'явитися і в CSV-файлі, і в keywords.
                # Для Viatec: CategorySpecsEnricher вже додав їх у spider →
                # guard not any(...) захищає від дублювання.
                if channel_config.virtual_specs:
                    injected_names = {vs["name"] for vs in channel_config.virtual_specs}
                    if not any(s.get("name", "").strip() in injected_names for s in specs):
                        specs = list(channel_config.virtual_specs) + list(specs)

                # 🔪 ОБРОБКА ДОВГИХ ХАРАКТЕРИСТИК
                current_description = cleaned.get("Опис", "")
                specs, updated_description = self.spec_length_handler.process_specifications(
                    specs, current_description
                )
                # ВАЖЛИВО: process_specifications() може вклеїти в опис сирий
                # вміст занадто довгих характеристик (_format_as_description),
                # обходячи TextSanitizer, який вже відпрацював у _clean_item()
                # ДО цього моменту. Тому санітайзимо ще раз — інакше markdown-
                # посилання/промо-сміття/заборонені слова з raw specs
                # потрапляють у фінальний CSV без очищення (виявлено на LP).
                cleaned["Опис"] = TextSanitizer.sanitize(
                    updated_description, supplier=config.supplier_name
                )
                
                specs = self._process_specs(specs, cleaned, adapter, spider)
                
                # Додаємо стандартні характеристики (Стан, Компанія-виробник, Країна-виробник)
                specs = SpecsEnricher.ensure_manufacturer_specs(specs, cleaned)
                
                # ---- POSTPROCESS SPECS ------------------------------- #
                
                category_id = channel_config.subdivision_id
                specs, guarantee_defaulted = RequiredGuaranteeService.ensure_guarantee(specs, category_id)
                if guarantee_defaulted:
                    self._inc_guarantee(output_file, category_id)
                specs = self.field_processor.process_specs_weight(specs, category_id, spider)
                specs = self.field_processor.process_specs_load_capacity(specs, spider)
                specs = self.field_processor.process_specs_hdd_capacity(specs, spider)
                specs = self.field_processor.process_specs_battery_capacity(specs, spider)
                
                # ---- DIMENSIONS -------------------------------------- #
                
                dimensions = self.field_processor.extract_dimensions_from_specs(specs, spider) or {}
                cleaned.update(dimensions)

                # ВАЖЛИВО: "Вага,кг" / "Ширина,см" / "Висота,см" / "Довжина,см"
                # вже нормалізовані з комою як десятковим розділювачем — так
                # само, як того вимагає PROM для всіх числових полів (див.
                # docstring ValidationService.sanitize_prom_numeric).
                # Раніше тут стояла конвертація коми назад у крапку — вона
                # трактувала вимогу PROM навпаки і повертала значення у
                # формат, який PROM відхиляє ("Тільки числові значення
                # дозволені"). Видалено — більше нічого тут конвертувати не
                # треба, dimensions вже готові до запису as-is.
                
                # ---- KEYWORDS ---------------------------------------- #

                if self.keywords_generator:
                    cleaned["Пошукові_запити"] = self.keywords_generator.generate_keywords(
                        cleaned.get("Назва_позиції", ""), category_id, specs, "ru"
                    )
                    cleaned["Пошукові_запити_укр"] = self.keywords_generator.generate_keywords(
                        cleaned.get("Назва_позиції_укр", ""), category_id, specs, "ua"
                    )
                
                # ---- WRITE ------------------------------------------- #

                # Шар 2: захисний обрізувач — гарантує ≤ PROM_HARD_LIMIT
                # зі збереженням обов'язкових хар-к (Стан, Виробник, Країна).
                specs = SpecLimitService.apply_limit(
                    specs, spider.logger, cleaned.get("Назва_позиції", "")
                )

                self._write_row(output_file, cleaned, specs)
                self.stats[output_file]["count"] += 1
            
            # Лічильник більше не потрібен — SkuCodeService керує кодами
        else:
            # Якщо немає мультиканального режиму - помилка
            spider.logger.error(f"⚠️ Мультиканальний режим не активовано для {spider.name}")
            raise DropItem("NO MULTI-CHANNEL")

        return item

    # ------------------------------------------------------------------ #
    # ANOMALY LOG
    # ------------------------------------------------------------------ #

    def _log_anomaly(self, product_code: str, retail: Decimal, price: Decimal) -> None:
        """
        Записує в anomal_price.log товари, де розрахована ціна перевищує роздріб постачальника.
        Формат: код товара = N; retail (від постачальника) = N; price (Ціна) = N
        """
        if not self._anomaly_log:
            return
        try:
            line = (
                f"код товара = {product_code}; "
                f"ретайл (від постачальника) = {int(retail)}; "
                f"price (Ціна) = {int(price)}\n"
            )
            self._anomaly_log.write(line)
        except Exception as e:
            if hasattr(self, "_spider_logger"):
                self._spider_logger.warning(f"⚠️ anomal_price.log: помилка запису: {e}")

    # ------------------------------------------------------------------ #
    # CSV
    # ------------------------------------------------------------------ #

    def _write_header(self, f):
        """Генерує CSV заголовок через PromCsvSchema"""
        header = PromCsvSchema.get_header(PROM_CSV_SPECS_LIMIT)
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
        for spec in specs[: PROM_CSV_SPECS_LIMIT]:
            row.extend([
                self.validation_service.sanitize_csv_value(spec.get("name", "")),
                self.validation_service.sanitize_csv_value(spec.get("unit", "")),
                self.validation_service.sanitize_csv_value(
                    self.validation_service.normalize_spec_value(spec.get("value", ""))
                ),
            ])
            written += 1

        # Заповнення порожніх характеристик
        for _ in range(PROM_CSV_SPECS_LIMIT - written):
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

    def _write_raw_row(self, output_file, row):
        """Записує вже готовий CSV-рядок без повторної обробки pipeline."""
        writer = csv.writer(self.files[output_file], delimiter=";", lineterminator="\n")
        writer.writerow(row)

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
            elif prom_field == "Посилання_зображення" and value:
                svc = self.image_services.get(spider.name)
                if svc:
                    value = svc.resolve_url(value)

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

        # Очищення заборонених слів, посилань та supplier-специфічних артефактів (Prom.ua).
        # supplier=config.supplier_name вмикає LP/Viatec-правила в TextSanitizer.
        supplier_config = self.configs.get(spider.name)
        supplier_name = supplier_config.supplier_name if supplier_config else None
        TextSanitizer.sanitize_item(result, supplier=supplier_name)

        return result

    # ------------------------------------------------------------------ #
    # SPECS
    # ------------------------------------------------------------------ #

    def _process_specs(self, specs, cleaned, adapter, spider):
        """
        Обробка характеристик через AttributeMapper + merge_all_specs.

        Шар 1 дедуплікації: якщо full_specs > PROM_HARD_LIMIT — замінюємо
        raw-версії змаплених хар-к їх Prom-еквівалентами (unmapped + mapped).
        Інформація не втрачається: mapped вже є нормалізованими Prom-версіями.
        Шар 2 (захисний обрізувач) застосовується далі через SpecLimitService.
        """
        if not self.attribute_mapper:
            return SpecsEnricher.ensure_condition(specs)

        cat = (
            adapter.get("Ідентифікатор_підрозділу", "")
            or cleaned.get("Ідентифікатор_підрозділу", "")
        )

        name_specs = self.attribute_mapper.map_product_name(
            cleaned.get("Назва_позиції", ""), cat
        )
        mapping_result = self.attribute_mapper.map_attributes(specs, cat)
        mapped   = mapping_result.get("mapped",   [])
        unmapped = mapping_result.get("unmapped", [])

        full_specs = merge_all_specs(specs, mapped, name_specs, spider.logger)

        if len(full_specs) <= PROM_HARD_LIMIT:
            return SpecsEnricher.ensure_condition(full_specs)

        # Перевищено ліміт: відкидаємо raw-версії хар-к, що вже представлені
        # у mapped (Prom-еквіваленти). Залишаємо unmapped + mapped + name_specs.
        product_name = cleaned.get("Назва_позиції", "")[:60]
        spider.logger.warning(
            f"⚠️ [SpecDedup] [{product_name}] "
            f"full={len(full_specs)} > {PROM_HARD_LIMIT} → "
            f"dedup: unmapped={len(unmapped)}, mapped={len(mapped)}, name={len(name_specs)}"
        )

        reduced = merge_all_specs(unmapped, mapped, name_specs, spider.logger)
        return SpecsEnricher.ensure_condition(reduced)

    # ------------------------------------------------------------------ #
    # STATS / CLOSE
    # ------------------------------------------------------------------ #

    def _inc(self, file, key):
        self.stats[file][key] += 1

    def _inc_guarantee(self, file: str, category_id: str) -> None:
        """Лічильник дефолтної гарантії (6 міс) по категоріях — для підсумкового логу в close_spider."""
        bucket = self.stats[file]["guarantee_defaults"]
        bucket[category_id] = bucket.get(category_id, 0) + 1

    def close_spider(self, spider):
        # Guard: Scrapy може викликати close_spider повторно при скасуванні.
        # Перевіряємо одразу, щоб уникнути double-close файлів і double-save.
        if self.stats_logged:
            return
        self.stats_logged = True

        for f in self.files.values():
            f.close()

        if self._anomaly_log:
            self._anomaly_log.close()
            self._anomaly_log = None

        # Зберігаємо sku_map та image-кеш на диск (per-supplier)
        for sku_service in self.sku_code_services.values():
            sku_service.save()
        for img_service in self.image_services.values():
            img_service.save_cache()

        # Виводимо статистику обробки характеристик
        self.spec_length_handler.print_stats()

        for file, s in self.stats.items():
            spider.logger.info(
                f"{file}: OK={s['count']} "
                f"NO_PRICE={s['filtered_no_price']} "
                f"NO_STOCK={s['filtered_no_stock']} "
                f"NO_SKU={s['filtered_no_sku']}"
            )
            RequiredGuaranteeService.log_summary(s.get("guarantee_defaults", {}), spider.logger)
