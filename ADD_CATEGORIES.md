# Додавання нової категорії

> 📘 **Передумова:** Якщо ви додаєте **нового постачальника** (не категорію), спочатку читайте `ADD_NEW_SUPPLIER.md`!

---

## 🎯 Архітектура генерації ключових слів

Кожен постачальник використовує СВІЙ процесор:
- `viatec` → `ViatecGenericProcessor`
- `secur` → `SecurGenericProcessor`
- `eserver` → `EServerGenericProcessor`

**Процес генерації складається з 3 блоків:**

1. **Блок 1: Модель і бренд** - автоматично додається процесором
2. **Блок 2: Характеристики** - ваш обробник категорії (створюєте ВИ)
3. **Блок 3: Universal phrases** - автоматично з CSV

### ⚠️ КРИТИЧНО: Ваш обробник повертає ТІЛЬКИ Блок 2!

**Додавайте ТІЛЬКИ:**
- ✅ `{base} {характеристика}` (наприклад: "патч панель utp")
- ✅ `{характеристика} {base}` (наприклад: "utp патч панель")
- ✅ `{base} {число} {одиниця}` (наприклад: "патч панель 24 портів")

**НЕ додавайте:**
- ❌ Модель/бренд (додає Блок 1)
- ❌ Universal phrases (додає Блок 3)
- ❌ Складні комбінації (створюються автоматично)

---

## 📋 Кроки додавання категорії

### Крок 1: Додати категорію в CSV

**Шлях:** `C:\FullStack\Scrapy\data\{supplier}\{supplier}_keywords.csv`

```csv
5092902;https://prom.ua/...;universal_phrases_ru;universal_phrases_ua;base_keyword_ru;base_keyword_ua;allowed_specs
```

**Поля:**
- `universal_phrases_ru/ua` - загальні фрази через кому
- `base_keyword_ru/ua` - базове ключове слово
- `allowed_specs` - **WHITE LIST** характеристик через кому

⚠️ **allowed_specs** - строгий white list з exact match!
- "Тип" дозволить ТІЛЬКИ "Тип", НЕ "Тип корпусу"
- "Порт" дозволить ТІЛЬКИ "Порт", НЕ "Кількість портів"

---

### Крок 2: Створити обробник категорії

**Шлях:** `C:\FullStack\Scrapy\keywords\categories\{supplier}\{назва}.py`

**Шаблон:**

```python
"""
Генератор ключових слів для {категорія}.
Категорія: {category_id}
"""

import re
from typing import List, Set

from keywords.core.helpers import SpecAccessor
from keywords.utils.spec_helpers import is_spec_allowed


def generate(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """
    Генерація ключових слів для {категорія}.
    
    Повертає ТІЛЬКИ Блок 2 (характеристики).

    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово
        allowed: Множина дозволених характеристик (строгий white list)

    Returns:
        Список ключових слів (тільки характеристики)
    """
    keywords = []

    # Характеристика 1
    if is_spec_allowed("Назва характеристики", allowed):
        value = accessor.value("Назва характеристики")
        if value:
            keywords.extend([
                f"{base} {value.lower()}",
                f"{value.lower()} {base}"
            ])

    # Характеристика з числом
    if is_spec_allowed("Кількість", allowed):
        count = accessor.value("Кількість")
        if count:
            match = re.search(r"(\d+)", count)
            if match:
                num = match.group(1)
                unit = "единиц" if lang == "ru" else "одиниць"
                keywords.append(f"{base} {num} {unit}")

    return keywords
```

**Правила:**
- ✅ Кожен `accessor.value()` **СТРОГО** через `is_spec_allowed()`
- ✅ `is_spec_allowed` використовує exact match (не fuzzy)
- ✅ Повертати ТІЛЬКИ `["{base} {spec}", "{spec} {base}"]`
- ❌ НЕ додавати модель/бренд/universal phrases/складні комбінації

---

### Крок 3: Зареєструвати в роутері постачальника

**Шлях:** `C:\FullStack\Scrapy\keywords\categories\{supplier}\router.py`

```python
from keywords.categories.{supplier} import existing_module, new_module

CATEGORY_HANDLERS = {
    "70704": existing_module.generate,
    "12345": new_module.generate,  # ← додати
}
```

---

### Крок 4: Експортувати модуль

**Шлях:** `C:\FullStack\Scrapy\keywords\categories\{supplier}\__init__.py`

```python
from keywords.categories.{supplier} import existing_module, new_module

__all__ = ["existing_module", "new_module", "router"]  # ← додати
```

---

## 🔄 Як це працює

```
Spider → Pipeline → ProductKeywordsGenerator(supplier="viatec") 
                            ↓
                    ViatecGenericProcessor
                            ↓
          categories/viatec/router.py
                            ↓
          categories/viatec/camera.py (ваш обробник)
```

**Процесор вибирається автоматично** за параметром `supplier` в `keywords/core/generator.py`.

---

## 🛠️ Корисні утиліти

```python
from keywords.utils.spec_helpers import (
    extract_capacity,      # Об'єм: "128gb" → {"value": 128, "formatted": "128gb"}
    extract_interface,     # Інтерфейс: "usb 3.0", "sata"
    extract_speed,         # Швидкість: "90" → "90"
    extract_rpm,          # Обороти: "7200" → "7200"
    is_spec_allowed       # Строга перевірка (exact match)
)
```

### ⚠️ Важливо про `is_spec_allowed`

**Строга перевірка (exact match):**
```python
allowed = {"тип", "кількість портів"}

is_spec_allowed("Тип", allowed)          # True  ✅
is_spec_allowed("Тип корпусу", allowed)  # False ❌
is_spec_allowed("Порт", allowed)         # False ❌
```

**Це запобігає:**
- ❌ "тип" → "тип корпусу", "тип пристрою"
- ❌ "порт" → "кількість портів", "порт живлення"
- ❌ Неочікувані збіги

---

## 📚 Приклади обробників

**Прості (1-2 характеристики):**
- `keywords/categories/viatec/usb_flash.py` - USB флешки
- `keywords/categories/viatec/battery.py` - Акумулятори

**Середні (3-5 характеристик):**
- `keywords/categories/eserver/panel.py` - Патч-панелі
- `keywords/categories/viatec/sd_card.py` - SD карти

**Складні (7+ характеристик):**
- `keywords/categories/viatec/hdd.py` - HDD диски
- `keywords/categories/viatec/camera.py` - Камери відеоспостереження
- `keywords/categories/eserver/cabinet.py` - Серверні шафи

---

## ✅ Чек-лист

- [ ] Категорія додана в CSV з `allowed_specs`
- [ ] Обробник створено за шаблоном
- [ ] Всі `accessor.value()` через `is_spec_allowed()`
- [ ] Розумію, що `is_spec_allowed` - exact match (не fuzzy)
- [ ] Повертається ТІЛЬКИ Блок 2
- [ ] Зареєстровано в `categories/{supplier}/router.py`
- [ ] Експортовано в `categories/{supplier}/__init__.py`
- [ ] Перевірка `check_compliance.py` пройдена ✅

---

## 🔍 Перевірка

```bash
python C:\FullStack\Scrapy\keywords\categories\{supplier}\check_compliance.py
```

Перевіряє, що обробник використовує тільки `allowed_specs` з CSV.

---

## ❌ Часті помилки

1. **Додавання universal phrases в обробник**
   - ❌ `keywords.append("патч панель для серверної шафи")`
   - ✅ Додати в CSV в поле `universal_phrases_ru`

2. **Додавання моделі/бренду**
   - ❌ `keywords.append(f"{brand} {model}")`
   - ✅ Додається автоматично (Блок 1)

3. **Складні комбінації**
   - ❌ `keywords.append(f"{base} {ports} {type}")`
   - ✅ Комбінуються автоматично

4. **Використання характеристик без перевірки**
   - ❌ `value = accessor.value("Тип")`
   - ✅ `if is_spec_allowed("Тип", allowed): value = accessor.value("Тип")`

5. **Очікування fuzzy match від `is_spec_allowed`**
   - ❌ Думати що "тип" дозволить "тип корпусу"
   - ✅ `is_spec_allowed` працює як exact match

6. **Неправильна локалізація**
   - ❌ `keywords.append(f"{base} портов")`
   - ✅ `unit = "портов" if lang == "ru" else "портів"`

---

## 🎯 Структура файлів

```
C:\FullStack\Scrapy\
├── data\{supplier}\
│   └── {supplier}_keywords.csv          # Крок 1
├── keywords\categories\{supplier}\
│   ├── {category}.py                    # Крок 2
│   ├── router.py                        # Крок 3
│   └── __init__.py                      # Крок 4
└── keywords\processors\{supplier}\
    └── generic.py                       # Процесор (вже створено)
```

---

## 🔙 Повернутися до створення постачальника

Якщо ви ще не створили постачальника, поверніться до:

📖 **`ADD_NEW_SUPPLIER.md`** - повна інструкція по створенню нового постачальника (структура, сервіси, spider, автоматична конфігурація через SupplierConfig)

---

## 📚 Додаткова документація

- **`ADD_NEW_SUPPLIER.md`** - додавання нового постачальника (автоматична конфігурація)
- **`suppliers/services/ARCHITECTURE.md`** - архітектура сервісів
- **`suppliers/PIPELINE_AUDIT.md`** - аудит pipeline

---

**Готово!** 🚀 Категорія додана, ключові слова генеруються!
