"""
Генератор ключових слів для товарів різних категорій.

Модульна архітектура:
- keywords/core/ - ядро системи (оркестрація, моделі, завантажувачі)
- keywords/processors/ - процесори для типів товарів (камери, DVR, загальні)
- keywords/categories/ - плагіни для специфічних категорій по постачальниках
- keywords/utils/ - утиліти для роботи з характеристиками та назвами

Приклад використання:
    >>> from keywords import ProductKeywordsGenerator
    >>> 
    >>> generator = ProductKeywordsGenerator(
    ...     keywords_csv_path="data/viatec/viatec_keywords.csv",
    ...     manufacturers_csv_path="data/viatec/viatec_manufacturers.csv"
    ... )
    >>> 
    >>> keywords = generator.generate_keywords(
    ...     product_name="Hikvision DS-2CD2143G0-I 2.8mm",
    ...     category_id="301105",
    ...     specs_list=[
    ...         {"name": "Виробник", "value": "Hikvision"},
    ...         {"name": "Роздільна здатність (Мп)", "value": "4"},
    ...     ],
    ...     lang="ru"
    ... )
"""

from keywords.core import ProductKeywordsGenerator

__version__ = "2.0.0"

__all__ = [
    "ProductKeywordsGenerator",
]
