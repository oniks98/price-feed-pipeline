"""
Конфігурація постачальників.
Централізує шляхи до файлів та налаштування для кожного постачальника.
"""
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from suppliers.constants import get_price_decimals, BASE_DATA_DIR


@dataclass
class SupplierConfig:
    """Конфігурація для одного постачальника"""
    
    # Основні ідентифікатори
    supplier_name: str  # Ім'я постачальника (viatec, secur, eserver, тощо)
    spider_name: str    # Повне ім'я spider (viatec_dealer, secur_retail, eserver_retail)
    mode: str           # Режим роботи (dealer, retail)
    
    # Базова директорія
    base_dir: Path
    data_dir: Path      # Директорія з даними постачальника
    
    # Шляхи до файлів (опціональні)
    category_file: Optional[Path] = None  # Основний файл категорій (з channel)
    mapping_rules_file: Optional[Path] = None
    keywords_file: Optional[Path] = None
    manufacturers_file: Optional[Path] = None
    # Прапорці функціональності
    use_multi_channel: bool = False  # Мультиканальний режим (site, prom, rozetka)
    use_keywords_generator: bool = False
    use_attribute_mapper: bool = False
    
    # Налаштування округлення ціни (з suppliers.constants)
    price_decimal_places: int = 0  # 0 = цілі (UAH), 2 = копійки (USD)
    
    @classmethod
    def from_spider(cls, spider_name: str, base_data_dir: Path = BASE_DATA_DIR):
        """
        Створює конфігурацію на основі імені spider
        
        Args:
            spider_name: Ім'я spider (наприклад, "eserver_retail", "viatec_dealer")
            base_data_dir: Базова директорія з даними
        
        Returns:
            SupplierConfig з автоматично визначеними шляхами
        """
        # Витягуємо ім'я постачальника та режим
        parts = spider_name.split('_')
        supplier_name = parts[0]
        mode = parts[1] if len(parts) > 1 else 'retail'
        
        base_dir = base_data_dir / supplier_name
        data_dir = base_data_dir / supplier_name
        
        # Автоматично визначаємо шляхи до файлів
        category_file = base_dir / f"{supplier_name}_category.csv"
        mapping_rules_file = base_dir / f"{supplier_name}_mapping_rules.csv"
        keywords_file = base_dir / f"{supplier_name}_keywords.csv"
        manufacturers_file = base_dir / f"{supplier_name}_manufacturers.csv"
        # Визначаємо режим роботи
        use_multi_channel = category_file.exists()
        use_keywords = keywords_file.exists() and manufacturers_file.exists()
        use_mapper = mapping_rules_file.exists()

        # manufacturers_file завжди передається якщо існує —
        # ManufacturersDB потрібен незалежно від keywords generator
        mfr_file = manufacturers_file if manufacturers_file.exists() else None

        # ✨ Округлення з централізованих констант
        # Передаємо повний spider_name (viatec_dealer), а не supplier_name (viatec)
        decimal_places = get_price_decimals(spider_name)
        
        return cls(
            supplier_name=supplier_name,
            spider_name=spider_name,
            mode=mode,
            base_dir=base_dir,
            data_dir=data_dir,
            category_file=category_file if use_multi_channel else None,
            mapping_rules_file=mapping_rules_file if use_mapper else None,
            keywords_file=keywords_file if use_keywords else None,
            manufacturers_file=mfr_file,
            use_multi_channel=use_multi_channel,
            use_keywords_generator=use_keywords,
            use_attribute_mapper=use_mapper,
            price_decimal_places=decimal_places,
        )
    
    def __repr__(self):
        """Зручне представлення для логування"""
        features = []
        if self.use_multi_channel:
            features.append("multi-channel")
        if self.use_keywords_generator:
            features.append("keywords")
        if self.use_attribute_mapper:
            features.append("mapper")
        
        features_str = ", ".join(features) if features else "basic"
        price_precision = f"price_dp={self.price_decimal_places}"
        return f"SupplierConfig(supplier={self.supplier_name}, features=[{features_str}], {price_precision})"
