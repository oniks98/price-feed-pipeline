# PriceFeedPipeline — довідник

---

## 🕷️ Парсинг (спайдери)

| Скрипт | Опис |
|---|---|
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

| Скрипт | Опис |
|---|---|
| `update_products.py` | Порівнює новий прайс зі старим, фіксує зміни цін, оновлює *_old.csv |
| `merge_csv.py` | Зливає всі постачальники в єдиний merged.csv |
| `merge_pending.py` | Зливає pending-файли (часткові оновлення) перед основним merge |
| `fetch_wholesale_csvs.py` | Завантажує оптові CSV-прайси від постачальників |
| `update_kod_product.py` | Оновлює / синхронізує Код_товару між файлами |
| `check_product_code.py` | Перевіряє унікальність і послідовність Код_товару в export-products.xlsx; лог → logs/check_product_code.log |

```bash
python scripts/update_products.py viatec dealer
python scripts/update_products.py viatec retail
python scripts/update_products.py secur retail

# Всі постачальники (dealer + retail) одразу
python scripts/update_products.py

python scripts/merge_csv.py
python scripts/check_product_code.py
```

---

## 📡 Генерація фідів маркетплейсів

| Скрипт | Опис |
|---|---|
| `feed_common.py` | Спільна бібліотека: завантаження XML, розрахунок цін, коефіцієнти, фільтрація — імпортується всіма генераторами |
| `generate_kasta_feed.py` | Генерує kasta_feed.xml (оптова ціна × коеф. категорії, fallback → DEFAULT_COEFFICIENT × XML-ціна) |
| `generate_epicenter_feed.py` | Генерує epicenter_feed.xml |
| `generate_rozetka_feed.py` | Генерує rozetka_feed.xml |

```bash
python scripts/generate_kasta_feed.py
python scripts/generate_epicenter_feed.py
python scripts/generate_rozetka_feed.py
```

Актуальні фіди (гілка `data-latest`):
```
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/kasta_feed.xml
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/epicenter_feed.xml
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/rozetka_feed.xml
```

---

## 🗂️ Маппінг категорій та атрибутів

| Скрипт | Опис |
|---|---|
| `export_prom_categories.py` | Синхронізує категорії з фіду Prom.ua з локальними файлами маркетплейсів |
| `fetch_epicenter_categories.py` | Завантажує актуальне дерево категорій Epicenter |
| `map_epicenter_categories.py` | Зіставляє категорії Prom ↔ Epicenter (rapidfuzz) |
| `map_epicenter_attributes.py` | Маппінг атрибутів товарів під формат Epicenter |
| `map_kasta_categories.py` | Зіставляє категорії Prom ↔ Kasta |

```bash
python scripts/export_prom_categories.py
python scripts/fetch_epicenter_categories.py
python scripts/map_epicenter_categories.py
python scripts/map_epicenter_attributes.py
python scripts/map_kasta_categories.py
```

---

## 🛒 Автоматизація Prom.ua

| Скрипт | Опис |
|---|---|
| `export_prom_cookies.py` | Витягує cookies активної сесії Prom і зберігає в prom_cookies.json |
| `prom_api_trigger.py` | Тригер імпорту товарів у Prom.ua через API після git push |
| `prom_noindex_automation.py` | Масово виставляє noindex на вказані товари |
| `prom_prosale_automation.py` | Масово додає ProSale до вказаних товарів |
| `prom_status.py` | Перевіряє поточний статус товарів на Prom.ua |

```bash
python scripts/export_prom_cookies.py
python scripts/prom_api_trigger.py
python scripts/prom_noindex_automation.py
python scripts/prom_prosale_automation.py
python scripts/prom_status.py
```

---

## 🧹 Очистка Kasta

| Скрипт | Опис |
|---|---|
| `kasta_bulk_select.py` | Масово знімає / виставляє товари на Kasta через браузер з debug-портом |

```bash
# 1. Запустити Chrome з debug-портом
"C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-port=9222 --user-data-dir="C:\chrome-kasta-debug"

# 2. Відкрити сторінку товарів Kasta
https://hub.kasta.ua/products?contract_id=bd045b2c-ceb9-4c9e-a3ba-cc414e5e76d9&status=OnSale&status=ZeroStock

# 3. Запустити скрипт
python scripts/kasta_bulk_select.py
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

| Дата | Size (GitHub API) |
|---|---|
| 12.04.26 | 16 744 |
| 19.04.26 | 41 847 |

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
