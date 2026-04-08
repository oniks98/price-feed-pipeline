# Додавання нового постачальника

## 🎯 Огляд архітектури

Додавання нового постачальника потребує **2 ручних реєстрації** + решта автоматично:

| Крок | Файл | Що робити |
|------|------|-----------|
| 1 | `scripts/update_products.py` | Додати рядок у `SUPPLIER_CONFIG` |
| 2 | `.github/workflows/pipeline.yml` | Додати рядок у `matrix.include` |
| 3 | `suppliers/constants.py` | Код, валюта, округлення |
| 4 | `data/newsupplier/` | CSV файли категорій |
| 5 | `suppliers/items.py` | Новий Item клас |
| 6 | `suppliers/spiders/newsupplier/` | Spider (без абсолютних шляхів!) |

**Що відбувається автоматично після реєстрації:**

- `update_products.py` (без аргументів) підхоплює нового постачальника з `SUPPLIER_CONFIG`
- `merge_csv.py` включає його у `merged.csv` для Prom.ua
- `SupplierConfig.from_spider(spider.name)` знаходить всі файли по імені паука
- `SkuCodeService` призначає стабільний `Код_товару` кожному SKU постачальника
- `ChannelService` розподіляє товари по каналах (site, prom...) з різними цінами

---

## Крок 1: Зареєструвати постачальника в SUPPLIER_CONFIG

**Файл:** `scripts/update_products.py`

Додайте один рядок у словник `SUPPLIER_CONFIG` — це **єдина точка реєстрації** нового постачальника для всього пайплайну (локального та GitHub Actions):

```python
SUPPLIER_CONFIG: dict[str, dict[str, str]] = {
    "viatec":      {"spider": "viatec_dealer",     "type": "dealer"},
    "secur":       {"spider": "secur_retail",      "type": "retail"},
    "eserver":     {"spider": "eserver_retail",    "type": "retail"},
    "newsupplier": {"spider": "newsupplier_retail", "type": "retail"},  # ← додати
}
```

> **spider** — ім'я павука для `ultra_clean_run.py`  
> **type** — тип для логіки diff у `update_products.py`

Після цього `update_products.py` (без аргументів) і `merge_csv.py` підхоплять нового постачальника **автоматично**.

---

## Крок 2: Додати павука в матрицю GitHub Actions

**Файл:** `.github/workflows/pipeline.yml`

Додайте рядок у секцію `matrix.include`:

```yaml
matrix:
  include:
    - supplier: viatec
      spider: viatec_dealer
    - supplier: secur
      spider: secur_retail
    - supplier: eserver
      spider: eserver_retail
    - supplier: newsupplier        # ← додати
      spider: newsupplier_retail   # ← додати
```

---

## Крок 3: Додати константи

**Файл:** `suppliers/constants.py`

Додайте постачальника у **три словники**:

```python
SUPPLIER_CODE_RANGES: Final[Mapping[str, int]] = {
    "viatec":      200000,
    "secur":       100100,
    "eserver":     600000,
    "neolight":    500000,
    "lun":         401001,
    "newsupplier": 700000,   # ← діапазон 700000–799999
}

PRICE_DECIMALS: Final[Mapping[str, int]] = {
    "viatec_dealer": 2,   # USD → копійки (123.45)
    "viatec_retail": 0,   # UAH → цілі (1235)
    "newsupplier":   0,   # UAH → цілі  (або 2 якщо USD/EUR)
}

SUPPLIER_CURRENCIES: Final[Mapping[str, str]] = {
    "viatec_dealer": "USD",
    "viatec_retail": "UAH",
    "newsupplier":   "UAH",
}
```

> **Правило діапазонів:** кожному постачальнику 100 000 кодів.  
> `sku_map.json` створюється автоматично при першому запуску паука — файл лічильника більше не потрібен.

---

## Крок 4: Створити структуру даних

```
data/newsupplier/
├── newsupplier_category.csv        # ОБОВ'ЯЗКОВО — канали, ціни, категорії ПРОМ
├── newsupplier_keywords.csv        # опціонально — ключові слова по категоріях
├── newsupplier_manufacturers.csv   # опціонально — виробники для ключових слів
└── newsupplier_mapping_rules.csv   # опціонально — маппінг характеристик
```

> `sku_map.json` з'явиться тут автоматично після першого запуску паука.

---

### 2.1 Головний файл — `newsupplier_category.csv` (обов'язковий)

Це **єдиний файл**, що замінює старі `coefficient_*.csv`, `personal_notes_*.csv`.  
Один рядок = один канал для однієї категорії постачальника.

```csv
№;Линк категории поставщика;channel;prefix;coefficient;Номер_групи;Назва_групи;Ідентифікатор_підрозділу;Посилання_підрозділу;Особисті_нотатки;Ярлик;Назва_Характеристики;Одиниця_виміру_Характеристики;Значення_Характеристики
1;https://newsupplier.com/category1;site;;"1,15";8950011;Назва групи на сайті;301105;https://prom.ua/category-link;site_note;;Вага;г;
;https://newsupplier.com/category1;prom;prom_;"1,25";140905382;Назва групи ПРОМ;301105;https://prom.ua/category-link;"prom_note, noindex";;;
```

**Ключові поля:**

| Поле                       | Опис                                                    |
| -------------------------- | ------------------------------------------------------- |
| `channel`                  | `site` або `prom` (або будь-який інший канал)           |
| `prefix`                   | Додається до `Ідентифікатор_товару` (наприклад `prom_`) |
| `coefficient`              | Коефіцієнт ціни через кому: `"1,25"`                    |
| `Ідентифікатор_підрозділу` | ID категорії на ПРОМ                                    |
| `Особисті_нотатки`         | Теги через кому: `"V, VMAX, noindex"`                   |

**Мінімум 2 рядки на категорію:** один для `site`, один для `prom`.

---

### 2.2 `newsupplier_keywords.csv` (опціонально)

```csv
Ідентифікатор_підрозділу;Посилання_підрозділу;universal_phrases_ru;universal_phrases_ua;base_keyword_ru;base_keyword_ua;allowed_specs
301105;https://prom.ua/category;камера,ip камера;камера,ip камера;камера;камера;Тип,Кількість портів
```

---

### 2.3 `newsupplier_manufacturers.csv` (опціонально)

```csv
Слово в названии продукта;Производитель (виробник)
hikvision;Hikvision
dahua;Dahua
```

---

### 2.4 `newsupplier_mapping_rules.csv` (опціонально)

Правила маппінгу характеристик постачальника → стандартні назви ПРОМ.  
Дивіться приклад: `data/viatec/viatec_mapping_rules.csv`

---

## Крок 5: Додати Item

**Файл:** `suppliers/items.py`

```python
class NewSupplierProductItem(scrapy.Item):
    """Item для товарів з newsupplier.com"""

    # Поля PROM (відповідають PromCsvSchema.BASE_FIELDS)
    Код_товару = scrapy.Field()
    Назва_позиції = scrapy.Field()
    Назва_позиції_укр = scrapy.Field()
    Пошукові_запити = scrapy.Field()
    Пошукові_запити_укр = scrapy.Field()
    Опис = scrapy.Field()
    Опис_укр = scrapy.Field()
    Тип_товару = scrapy.Field()
    Ціна = scrapy.Field()
    Валюта = scrapy.Field()
    Одиниця_виміру = scrapy.Field()
    Посилання_зображення = scrapy.Field()
    Наявність = scrapy.Field()
    Кількість = scrapy.Field()
    Назва_групи = scrapy.Field()
    Назва_групи_укр = scrapy.Field()
    Номер_групи = scrapy.Field()
    Ідентифікатор_товару = scrapy.Field()
    Ідентифікатор_підрозділу = scrapy.Field()
    Посилання_підрозділу = scrapy.Field()
    Особисті_нотатки = scrapy.Field()
    Ярлик = scrapy.Field()
    Виробник = scrapy.Field()
    Країна_виробник = scrapy.Field()
    Вага_кг = scrapy.Field()
    Ширина_см = scrapy.Field()
    Висота_см = scrapy.Field()
    Довжина_см = scrapy.Field()
    Продукт_на_сайті = scrapy.Field()
    Пошукові_запити_укр = scrapy.Field()

    # Технічні поля (не потрапляють у CSV)
    price_type = scrapy.Field()
    supplier_id = scrapy.Field()
    output_file = scrapy.Field()
    specifications_list = scrapy.Field()
    category_url = scrapy.Field()           # для ChannelService
```

> **Порада:** Скопіюйте повний Item з `ViatecProductItem` — він містить усі 47 полів.

---

## Крок 6: Створити Spider

**Структура:**

```
suppliers/spiders/newsupplier/
├── __init__.py
└── retail.py       # або dealer.py
```

**Шаблон spider** (`retail.py`):

> **Важливо:** НЕ використовуйте абсолютні шляхи `C:\...` у павуках.  
> Шляхи до CSV файлів та `.env` завжди визначайте через `PROJECT_ROOT` env:  
> `Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy")) / "data" / ...`  
> Це забезпечує роботу як локально, так і в GitHub Actions.

```python
import os
import scrapy
from pathlib import Path
from suppliers.items import NewSupplierProductItem


class NewSupplierRetailSpider(scrapy.Spider):
    name = "newsupplier_retail"          # формат: {supplier}_{mode}
    allowed_domains = ["newsupplier.com"]
    start_urls = ["https://newsupplier.com/catalog"]
    output_filename = "newsupplier_new.csv"

    # Правило іменування вихідного файлу (ultra_clean_run.py):
    # {supplier_name}_new.csv, де supplier_name = перша частина імені до '_'
    # newsupplier_retail → newsupplier_new.csv  (автоматично, нічого не міняти)

    custom_settings = {
        "ITEM_PIPELINES": {
            "suppliers.pipelines.SuppliersPipeline": 300,
        }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Шлях до CSV завжди через PROJECT_ROOT — працює локально і в CI
        _root = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy"))
        self.category_mapping = self._load_category_mapping(_root)

    def _load_category_mapping(self, root: Path) -> dict:
        csv_path = root / "data" / "newsupplier" / "newsupplier_category.csv"
        mapping = {}
        # ... читання CSV ...
        return mapping

    def parse(self, response):
        for product in response.css(".product"):
            item = NewSupplierProductItem()

            item["Назва_позиції"] = product.css(".title::text").get("").strip()
            item["Ціна"] = product.css(".price::text").get("").strip()
            item["Наявність"] = product.css(".stock::text").get("").strip()
            item["Ідентифікатор_товару"] = product.css(".sku::text").get("").strip()
            item["Посилання_зображення"] = product.css("img::attr(src)").get("")

            # Технічні поля — обов'язкові для pipeline
            item["price_type"] = "retail"
            item["supplier_id"] = "newsupplier"
            item["output_file"] = self.output_filename
            item["category_url"] = response.url   # ← потрібен для ChannelService

            item["specifications_list"] = []       # список {"name","value","unit"}

            yield item
```

**Якщо павук потребує авторизації** (як `viatec_dealer`):

```python
from dotenv import load_dotenv

def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    _root = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\Scrapy"))

    # load_dotenv НЕ перезаписує вже існуючі env-змінні.
    # Локально: читає з suppliers/.env
    # GitHub Actions: .env немає, але secrets вже в env — працює автоматично
    load_dotenv(_root / "suppliers" / ".env")
    self.email = os.getenv("NEWSUPPLIER_EMAIL")
    self.password = os.getenv("NEWSUPPLIER_PASSWORD")

    if not self.email or not self.password:
        raise ValueError(
            "❌ Відсутні NEWSUPPLIER_EMAIL / NEWSUPPLIER_PASSWORD. "
            "Локально: додайте в suppliers/.env. CI: додайте в GitHub Secrets."
        )
```

> Додайте `NEWSUPPLIER_EMAIL` та `NEWSUPPLIER_PASSWORD` в GitHub Secrets  
> та передайте їх у `pipeline.yml` → `env:` блок кроку `Run spider`.

> **Приклади для копіювання:**
>
> - `suppliers/spiders/secur/retail.py` — сучасний retail spider
> - `suppliers/spiders/viatec/dealer.py` — spider з авторизацією та мультиканалом

---

## Крок 7: Налаштувати генерацію ключових слів (опціонально)

Якщо потрібні пошукові запити — створіть процесор та категорії.

### 7.1 Структура файлів

```
keywords/processors/newsupplier/
├── __init__.py
├── base.py
└── generic.py

keywords/categories/newsupplier/
├── __init__.py
└── router.py
```

### 7.2 `base.py`

```python
from keywords.processors.base import BaseProcessor
from keywords.core.models import MAX_UNIVERSAL_KEYWORDS
from keywords.core.loaders import CategoryConfig


class NewSupplierBaseProcessor(BaseProcessor):
    def _generate_universal_keywords(self, config: CategoryConfig, lang: str):
        phrases = getattr(config, f"universal_phrases_{lang}", [])
        return phrases[:MAX_UNIVERSAL_KEYWORDS]
```

### 7.3 `router.py`

```python
from typing import Optional, Callable

CATEGORY_HANDLERS = {
    # "301105": some_category.generate,
}

def get_category_handler(category_id: str) -> Optional[Callable]:
    return CATEGORY_HANDLERS.get(category_id)
```

### 7.4 Зареєструвати в `generator.py`

**Файл:** `keywords/core/generator.py`

```python
from keywords.processors.newsupplier.generic import GenericProcessor as NewSupplierGenericProcessor

class ProductKeywordsGenerator:
    def __init__(self, ...):
        self.processors = {
            "viatec":      ViatecGenericProcessor(),
            "secur":       SecurGenericProcessor(),
            "newsupplier": NewSupplierGenericProcessor(),  # ← додати
        }
```

> Детальніше про категорії та процесори — в `ADD_CATEGORIES.md`

---

## Як це все працює разом

```
scrapy crawl newsupplier_retail
         │
         ▼
SupplierConfig.from_spider("newsupplier_retail")
  supplier_name = "newsupplier"
  category_file = data/newsupplier/newsupplier_category.csv  ✓
  keywords_file = data/newsupplier/newsupplier_keywords.csv  ✓
  ...
         │
         ▼
SkuCodeService(
  map_file  = data/newsupplier/sku_map.json,   # автостворюється
  start_code = 700000                           # з SUPPLIER_CODE_RANGES
)
         │
         ▼
ChannelService(newsupplier_category.csv)
  1 товар → 2 записи (site + prom)
  з різними цінами, категоріями, нотатками
         │
         ▼
data/output/newsupplier_new.csv
```

---

## ✅ Чек-лист

### Обов'язково:

- [ ] `SUPPLIER_CONFIG` в `scripts/update_products.py` — додати рядок
- [ ] `matrix.include` в `.github/workflows/pipeline.yml` — додати рядок
- [ ] `SUPPLIER_CODE_RANGES` — новий діапазон в `constants.py`
- [ ] `PRICE_DECIMALS` — округлення ціни в `constants.py`
- [ ] `SUPPLIER_CURRENCIES` — валюта в `constants.py`
- [ ] `data/newsupplier/newsupplier_category.csv` — канали та коефіцієнти
- [ ] `data/newsupplier/newsupplier_old.csv` — початковий baseline для diff (після першого запуску)
- [ ] Item у `suppliers/items.py`
- [ ] Spider у `suppliers/spiders/newsupplier/` — **без абсолютних шляхів** (через `PROJECT_ROOT`)

### Опціонально:

- [ ] `newsupplier_keywords.csv` + `newsupplier_manufacturers.csv` — ключові слова
- [ ] `newsupplier_mapping_rules.csv` — маппінг характеристик
- [ ] Процесор у `keywords/processors/newsupplier/`
- [ ] Роутер у `keywords/categories/newsupplier/`
- [ ] Реєстрація в `keywords/core/generator.py`
- [ ] GitHub Secrets для credentials (якщо павук потребує авторизації)

---

## 🔍 Перевірка після запуску

**Логи паука:**

```
📦 SupplierConfig(supplier=newsupplier, features=[multi-channel, keywords], price_dp=0)
📋 sku_map не знайдено (sku_map.json), починаємо новий
🆕 Новий SKU [NS-12345] → Код 700000
🆕 Новий SKU [NS-12346] → Код 700001
💾 sku_map збережено: 10 записів → sku_map.json
✅ YIELD [site]: Назва товару | Ціна: 1250 | Характеристик: 12
✅ YIELD [prom]: Назва товару | Ціна: 1350 | Характеристик: 12
```

**Файли після завершення:**

```
data/output/newsupplier_new.csv    ← результат парсингу
data/newsupplier/sku_map.json      ← автоствоєний словник SKU→Код
```

**Запуск скрипту оновлення:**

```bash
# Конкретний постачальник:
python scripts/update_products.py newsupplier retail

# Всі постачальники з SUPPLIER_CONFIG (автоматично підхоплює нового):
python scripts/update_products.py
```

> `newsupplier` буде підхоплено автоматично після додавання в `SUPPLIER_CONFIG`.  
> Окремо редагувати список `SUPPLIERS` **не потрібно** — він генерується з `SUPPLIER_CONFIG`.
