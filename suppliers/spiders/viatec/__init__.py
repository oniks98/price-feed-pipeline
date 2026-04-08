"""
Viatec spiders package
Містить пауки для парсингу viatec.ua (retail і dealer)
"""
from .retail import ViatecRetailSpider
from .dealer import ViatecDealerSpider

__all__ = ['ViatecRetailSpider', 'ViatecDealerSpider']
