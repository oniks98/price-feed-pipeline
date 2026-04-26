# PriceFeedPipeline — довідник

---

## 🕷️ Парсинг (спайдери)

| Скрипт               | Опис                                                           |
| -------------------- | -------------------------------------------------------------- |
| `ultra_clean_run.py` | Запускає Scrapy-спайдер із повним очищенням кешу перед стартом |

```bash
python scripts/ultra_clean_run.py viatec_dealer
python scripts/ultra_clean_run.py viatec_retail
python scripts/ultra_clean_run.py secur_retail
python scripts/ultra_clean_run.py secur_feed
python scripts/ultra_clean_run.py secur_feed_full
```

---

## 📦 Обробка товарів

| Скрипт                    | Опис                                                                                    |
| ------------------------- | --------------------------------------------------------------------------------------- |
| `update_products.py`      | Порівнює новий прайс зі старим, фіксує зміни цін, оновлює \*\_old.csv                   |
| `prom_merge_csv.py`       | Об'єднує всіх постачальників в єдиний merged.csv                                        |
| `prom_merge_pending.py`   | Об'єднує merged_prev.csv з новим merged.csv якщо є незастосовані дані (retry logic)     |
| `copy_csvs_main.py`       | Витягує \*\_old.csv з гілки data-latest для локального тестування                       |
| `products_update_code.py` | Оновлює Код\_товару в export-products.xlsx по sku\_map.json                             |
| `products_check_code.py`  | Перевіряє унікальність і послідовність Код\_товару в export-products.xlsx; лог → logs/  |
| `change_image.py`         | Копіює зображення з base-рядка у prom\_-рядок в export-products.xlsx                   |

```bash
python scripts/update_products.py viatec dealer
python scripts/update_products.py viatec retail
python scripts/update_products.py secur retail

# Всі постачальники (dealer + retail) одразу
python scripts/update_products.py

python scripts/products_update_code.py
python scripts/change_image.py

python scripts/prom_merge_csv.py
python scripts/products_check_code.py
```

---

## 📡 Генерація фідів маркетплейсів

| Скрипт                       | Опис                                                                                                            |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `generate_utils_feed.py`     | Спільна бібліотека: завантаження XML, розрахунок цін, коефіцієнти — імпортується всіма генераторами (не запускати напряму) |
| `generate_merchant_feed.py`  | Збагачує Google Merchant XML-фід мітками custom_label (theme / segment / price / schedule)                      |
| `rule_merchant_center.py`    | Генерує CSV-правила для generate_merchant_feed.py (theme, schedule, google_cat_id)                              |
| `generate_kasta_feed.py`     | Генерує kasta_feed.xml (оптова ціна × коеф. категорії, fallback → DEFAULT_COEFFICIENT × XML-ціна)               |
| `generate_epicenter_feed.py` | Генерує epicenter_feed.xml                                                                                      |
| `generate_rozetka_feed.py`   | Генерує rozetka_feed.xml                                                                                        |

```bash
python scripts/rule_merchant_center.py
python scripts/generate_merchant_feed.py
python scripts/generate_kasta_feed.py
python scripts/generate_epicenter_feed.py
python scripts/generate_rozetka_feed.py
```

Актуальні фіди (гілка `data-latest`):

```
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/merchant_feed.xml
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/kasta_feed.xml
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/epicenter_feed.xml
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/rozetka_feed.xml
```

---

## 🗂️ Маппінг категорій та атрибутів

| Скрипт                           | Опис                                                                    |
| --------------------------------- | ----------------------------------------------------------------------- |
| `prom_export_categories.py`      | Синхронізує категорії з фіду Prom.ua з локальними файлами маркетплейсів |
| `epicenter_export_categories.py` | Завантажує актуальне дерево категорій Epicenter                         |
| `epicenter_map_categories.py`    | Зіставляє категорії Prom ↔ Epicenter (rapidfuzz)                        |
| `epicenter_map_attributes.py`    | Зіставляє атрибути Epicenter з параметрами Prom (фаззі-матчинг ≥ 80%)  |
| `kasta_map_categories.py`        | Зіставляє категорії Prom ↔ Kasta                                        |
| `kasta_export_coef.py`           | Розраховує coef_kasta з mappings.xlsx + royalty.xlsx і записує в markets_coefficients.csv |

```bash
python scripts/prom_export_categories.py
python scripts/epicenter_export_categories.py
python scripts/epicenter_map_categories.py
python scripts/epicenter_map_attributes.py
python scripts/kasta_map_categories.py
python scripts/kasta_export_coef.py
```

---

## 🛒 Автоматизація Prom.ua

| Скрипт                       | Опис                                                                                              |
| ---------------------------- | ------------------------------------------------------------------------------------------------- |
| `prom_export_cookies.py`     | Витягує cookies активної сесії Prom і зберігає в prom_cookies.json (запускати локально)           |
| `prom_api_trigger.py`        | Тригер імпорту товарів у Prom.ua через API після git push                                         |
| `prom_noindex_automation.py` | Масово виставляє noindex на вказані товари через браузер                                           |
| `prom_prosale_automation.py` | Масово додає ProSale до вказаних товарів через браузер                                            |
| `prom_import_status.py`      | Бібліотека читання/запису статусу імпорту Prom — імпортується в prom_merge_pending і prom_api_trigger (не запускати напряму) |

```bash
python scripts/prom_export_cookies.py
python scripts/prom_api_trigger.py
python scripts/prom_noindex_automation.py
python scripts/prom_prosale_automation.py
```

---

## 🧹 Очистка Kasta

| Скрипт                     | Опис                                                                        |
| -------------------------- | --------------------------------------------------------------------------- |
| `kasta_delete_products.py` | Масово відмічає товари для видалення на Kasta через браузер з debug-портом  |

```bash
# 1. Запустити Chrome з debug-портом
"C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\chrome-kasta-debug"

# 2. Відкрити сторінку товарів Kasta
https://hub.kasta.ua/products?contract_id=bd045b2c-ceb9-4c9e-a3ba-cc414e5e76d9&status=OnSale&status=ZeroStock

# 3. Запустити скрипт
python scripts/kasta_delete_products.py
```

---

## 🔧 Git — корисні команди

```bash
# Видалити коміт бота з remote (локальний код не чіпає)
git push origin HEAD:main --force

# Заховати локальні зміни, підтягнути remote, повернути зміни
git stash
git pull --rebase
git stash pop

# Скасувати локальний коміт без втрати змін
git reset --soft origin/main

# Повністю скинути до remote (НЕБЕЗПЕЧНО — видаляє незбережені зміни)
git reset --hard HEAD~1
git clean -fd
git pull --rebase
git push --force
```

### Заміна файлів у гілці data-latest вручну

```bash
git fetch origin
git reset --hard origin/data-latest   # скинути до актуального remote
git log --oneline -1                  # перевірити хеш

# скопіювати/замінити файли в data/

git add data/secur/
git commit --amend --no-edit
git push origin data-latest --force
```

---

## 📊 Розмір репозиторію (для контролю)

| Дата     | Size (GitHub API) |
| -------- | ----------------- |
| 12.04.26 | 16 744            |
| 19.04.26 | 41 847            |

```
https://api.github.com/repos/oniks98/price-feed-pipeline
```

---

## 🔣 Сортування мінус-слів (Google Таблиці)

```
=SORT(UNIQUE(TOCOL(SPLIT(A:A;" ");1)))
=SORT(UNIQUE(TOCOL(SPLIT(REGEXREPLACE(A:A;"[!@%,*]";"");" ");1)))
=SORT(UNIQUE(TOCOL(SPLIT(REGEXREPLACE(A:A;"[!@%,*\.\(\)\[\]]";"");" ");1)))
```

---

## 📦 Встановлення залежностей

```bash
pip install openpyxl rapidfuzz requests
```
