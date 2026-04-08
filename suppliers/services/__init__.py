"""
Сервіси для обробки даних у pipeline

Використання:
    from suppliers.services import (
        SupplierConfig,
        ChannelService,
        AvailabilityService,
        FieldProcessor,
        ValidationService,
        merge_all_specs,
        PromCsvSchema,
    )
"""

from .supplier_config import SupplierConfig
from .channel_service import ChannelService
from .availability_service import AvailabilityService
from .field_processor import FieldProcessor
from .validation_service import ValidationService
from .specs_utils import merge_all_specs, merge_specs, should_replace_attribute
from .prom_csv_schema import PromCsvSchema
from .category_specs_enricher import CategorySpecsEnricher
from .spec_length_handler import SpecificationLengthHandler, process_long_specifications
from .text_sanitizer import TextSanitizer

__all__ = [
    # Конфігурація
    "SupplierConfig",
    
    # Обробка цін і наявності
    "ChannelService",
    "AvailabilityService",
    
    # Обробка полів і валідація
    "FieldProcessor",
    "ValidationService",
    
    # Обробка характеристик
    "merge_all_specs",
    "merge_specs",
    "should_replace_attribute",
    "CategorySpecsEnricher",
    "SpecificationLengthHandler",
    "process_long_specifications",
    
    # Схема CSV
    "PromCsvSchema",

    # Очищення тексту
    "TextSanitizer",
]
