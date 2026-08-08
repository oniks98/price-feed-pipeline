# PriceFeedPipeline — довідник

---

## 🕷️ Парсинг (спайдери)

| Скрипт               | Опис                                                           |
| -------------------- | -------------------------------------------------------------- |
| `ultra_clean_run.py` | Запускає Scrapy-спайдер із повним очищенням кешу перед стартом |

```bash
python scripts/ultra_clean_run.py viatec_dealer
python scripts/ultra_clean_run.py secur_feed
python scripts/ultra_clean_run.py lp_api

python scripts/ultra_clean_run.py viatec_feed_full  - недоделан, не проставлены категории под фид в category.csv
python scripts/ultra_clean_run.py viatec_retail
python scripts/ultra_clean_run.py secur_retail
python scripts/ultra_clean_run.py secur_feed_full  - для хар-к аякс
```

---

## Додавання нових категорій

# Від ПРОМу

python scripts/prom_export_categories.py
python scripts/markets_export_coef.py
python scripts/epicenter_attr_pipeline.py
python scripts/merchant_rule.py

# Від LP

python scripts/lp_export_categories.py

## 📦 Обробка товарів

| Скрипт                    | Опис                                                                                  |
| ------------------------- | ------------------------------------------------------------------------------------- |
| `update_products.py`      | Порівнює новий прайс зі старим, фіксує зміни цін, оновлює \*\_old.csv                 |
| `prom_merge_csv.py`       | Об'єднує всіх постачальників в єдиний merged.csv                                      |
| `prom_merge_pending.py`   | Об'єднує merged_prev.csv з новим merged.csv якщо є незастосовані дані (retry logic)   |
| `products_copy_csvs_main.py` | Витягує \*\_old.csv з гілки data-latest для локального тестування                  |
| `products_update_code.py` | Оновлює Код_товару в export-products.xlsx по sku_map.json                             |
| `products_check_code.py`  | Перевіряє унікальність і послідовність Код_товару в export-products.xlsx; лог → logs/ |
| `change_image.py`         | Копіює зображення з base-рядка у prom\_-рядок в export-products.xlsx                  |

```bash
python scripts/update_products.py viatec dealer
python scripts/update_products.py viatec retail
python scripts/update_products.py secur retail
python scripts/update_products.py lp dealer

# Всі постачальники (dealer + retail) одразу
python scripts/update_products.py

python scripts/products_update_code.py
python scripts/change_image.py

python scripts/prom_merge_csv.py
python scripts/products_check_code.py
```

---

## 📡 Генерація фідів маркетплейсів

| Скрипт                       | Опис                                                                                                                       |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| `generate_utils_feed.py`     | Спільна бібліотека: завантаження XML, розрахунок цін, коефіцієнти — імпортується всіма генераторами (не запускати напряму) |
| `generate_merchant_feed.py`  | Збагачує Google Merchant XML-фід мітками custom_label (theme / segment / price / schedule)                                 |
| `rule_merchant_center.py`    | Генерує CSV-правила для generate_merchant_feed.py (theme, schedule, google_cat_id)                                         |
| `generate_kasta_feed.py`     | Генерує kasta_feed.xml (оптова ціна × коеф. категорії, fallback → DEFAULT_COEFFICIENT × XML-ціна)                          |
| `generate_epicenter_feed.py` | Генерує epicenter_feed.xml                                                                                                 |
| `generate_rozetka_feed.py`   | Генерує rozetka_feed.xml                                                                                                   |

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

| Скрипт                           | Опис                                                                                      |
| -------------------------------- | ----------------------------------------------------------------------------------------- |
| `prom_export_categories.py`      | Синхронізує категорії з фіду Prom.ua з локальними файлами маркетплейсів                   |
| `epicenter_export_categories.py` | Завантажує актуальне дерево категорій Epicenter                                           |
| `epicenter_map_categories.py`    | Зіставляє категорії Prom ↔ Epicenter (rapidfuzz)                                          |
| `epicenter_map_attributes.py`    | Зіставляє атрибути Epicenter з параметрами Prom (фаззі-матчинг ≥ 80%)                     |
| `kasta_map_categories.py`        | Зіставляє категорії Prom ↔ Kasta                                                          |
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

| Скрипт                       | Опис                                                                                                                         |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `prom_export_cookies.py`     | Витягує cookies активної сесії Prom і зберігає в prom_cookies.json (запускати локально)                                      |
| `prom_api_trigger.py`        | Тригер імпорту товарів у Prom.ua через API після git push                                                                    |
| `prom_noindex_automation.py` | Масово виставляє noindex на вказані товари через браузер                                                                     |
| `prom_prosale_automation.py` | Масово додає ProSale до вказаних товарів через браузер                                                                       |
| `prom_import_status.py`      | Бібліотека читання/запису статусу імпорту Prom — імпортується в prom_merge_pending і prom_api_trigger (не запускати напряму) |

```bash
python scripts/prom_export_cookies.py
python scripts/prom_api_trigger.py
python scripts/prom_noindex_automation.py
python scripts/prom_prosale_automation.py
```

---

## 🧹 Очистка Kasta

| Скрипт                     | Опис                                                                       |
| -------------------------- | -------------------------------------------------------------------------- |
| `kasta_delete_products.py` | Масово відмічає товари для видалення на Kasta через браузер з debug-портом |

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

12.05 57 000
14.06 165262

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

---

## 🆘 Ручний аварійний запуск повного пайплайну

Цей сценарій призначений для випадку, коли GitHub Actions не запускається, але
GitHub і зовнішні сервіси доступні. Виконувати команди потрібно з кореня
репозиторію `/c/FullStack/PriceFeedPipeline` у **Git Bash** (термінал VS Code).

### Ключові правила

- Поки виконується цей сценарій, не запускати `pipeline.yml` або
  `pipeline_merchant_feed.yml` і не давати іншій людині писати у
  `data-latest`. Ця гілка оновлюється через `--amend` + `--force`, тому
  одночасні записи можуть втратити дані.
- Не переключати робочу копію з `main` на `data-latest`. `main` містить код,
  конфігурацію, `sku_map.json` та `image_cache.json`; `data-latest` — лише
  великі згенеровані файли. Для публікації використовується окремий клон.
- Не використовувати `git reset --hard` для цього сценарію. Якщо `git status`
  показує незрозумілі зміни, спочатку розібрати їх або зберегти окремим
  комітом/stash, а не змішувати з аварійним запуском.
- Після помилки паука не публікувати його неповний результат. Нижче для цього
  передбачено змінні `*_OK=false`.

### 0. Підготовка та синхронізація

Переконатися, що локальна гілка `main` чиста, оновити її та локальне
посилання на `data-latest`:

```bash
git switch main
git status --short
git pull --ff-only origin main
git fetch origin +data-latest:refs/remotes/origin/data-latest
git branch -f data-latest origin/data-latest

export PROJECT_ROOT="$(pwd)"
python -m pip install -r requirements.txt
playwright install chromium
```

`git status --short` має бути порожнім до початку роботи. Команда `git branch
-f` лише оновлює локальне посилання, яке читає
`products_copy_csvs_main.py`; робоча гілка залишається `main`.

Перед кожним ручним запуском прибрати **лише** попередні проміжні результати.
`*_old.csv`, `sku_map.json` та `image_cache.json` не видаляти:

```bash
rm -f \
  data/output/viatec_new.csv \
  data/output/secur_new.csv \
  data/output/lp_new.csv \
  data/output/viatec_status.txt \
  data/output/secur_status.txt \
  data/output/lp_status.txt \
  data/viatec/import_products.csv \
  data/secur/import_products.csv \
  data/lp/import_products.csv \
  data/merged.csv \
  data/merged_prev.csv \
  data/prom_import_status.json
unset VIATEC_OK SECUR_OK LP_OK
```

### 1. Відновити актуальні базові CSV

Запустити скрипт **після** синхронізації `data-latest` вище. Переходити в
`data-latest` для цього не потрібно:

```bash
python scripts/products_copy_csvs_main.py
ls -lh data/viatec/viatec_old.csv data/secur/secur_old.csv data/lp/lp_old.csv
```

Усі три `*_old.csv` мають бути отримані. Це базова версія для порівняння
нового прайсу; без неї `update_products.py` не можна запускати.

### 2. Запустити пауків і перевірити їхній результат

Пауків можна запускати послідовно — це безпечніше для локального аварійного
режиму:

```bash
python scripts/ultra_clean_run.py viatec_dealer
python scripts/ultra_clean_run.py secur_feed
python scripts/ultra_clean_run.py lp_api

cat data/output/*_status.txt
```

Для кожного паука потрібні `*_status.txt = success` і відповідний
`data/output/{supplier}_new.csv`. Якщо паук завершився з помилкою, не
запускати його обробку: перед наступним кроком задати змінну для конкретного
постачальника, наприклад:

```bash
export VIATEC_OK=false # якщо неуспішний viatec_dealer
export SECUR_OK=false  # якщо неуспішний secur_feed
export LP_OK=false     # якщо неуспішний lp_api
```

Для успішних постачальників змінні не задавати. Їхні дані обробляться, а
стан неуспішного постачальника залишиться без змін у `data-latest`.

### 3. Сформувати дані для Prom

```bash
python scripts/update_products.py
python scripts/prom_merge_csv.py
```

Перевірити статистику `update_products.py` і наявність `data/merged.csv`.
Якщо `prom_merge_csv.py` повідомив, що даних для злиття немає, нового файлу
для Prom немає: пропустити кроки 4, 6 і 7, за потреби закомітити кеші у кроці 5 і
не запускати Prom-автоматизації для цього циклу.

Перед обробкою pending-даних клонувати поточний стан `data-latest` в окрему
тимчасову директорію. Змінна `$publish_dir` використовується у наступних
кроках, тому їх слід виконувати в тій самій сесії Git Bash:

```bash
remote="$(git remote get-url origin)"
publish_dir="$(mktemp -d -t price-feed-pipeline-data-latest.XXXXXX)"
git clone --branch data-latest --single-branch "$remote" "$publish_dir"

if [[ -f "$publish_dir/data/merged_prev.csv" ]]; then
  cp "$publish_dir/data/merged_prev.csv" data/merged_prev.csv
fi
if [[ -f "$publish_dir/data/prom_import_status.json" ]]; then
  cp "$publish_dir/data/prom_import_status.json" data/prom_import_status.json
fi

python scripts/prom_merge_pending.py
```

`prom_merge_pending.py` є обов'язковим: якщо попередній імпорт Prom був
невдалий, він додає незастосовані зміни з `merged_prev.csv` до нового
`merged.csv`.

### 4. Опублікувати CSV у `data-latest`

Спочатку ротувати попередній `merged.csv`, потім скопіювати новий `merged.csv`,
`*_old.csv` та `import_products.csv` у окремий клон. Не копіювати файли в
робочу копію гілки `main` і не переключати її.

```bash
if [[ -f "$publish_dir/data/merged.csv" ]]; then
  cp "$publish_dir/data/merged.csv" "$publish_dir/data/merged_prev.csv"
fi
cp data/merged.csv "$publish_dir/data/merged.csv"

for supplier in viatec secur lp; do
  target_dir="$publish_dir/data/$supplier"
  mkdir -p "$target_dir"
  cp "data/$supplier/${supplier}_old.csv" "$target_dir/${supplier}_old.csv"
  if [[ -f "data/$supplier/import_products.csv" ]]; then
    cp "data/$supplier/import_products.csv" "$target_dir/import_products.csv"
  fi
done

git -C "$publish_dir" config user.name "manual-pipeline"
git -C "$publish_dir" config user.email "manual-pipeline@users.noreply.github.com"
git -C "$publish_dir" add -- data
if ! git -C "$publish_dir" diff --cached --quiet; then
  git -C "$publish_dir" commit --amend --no-edit
  git -C "$publish_dir" push origin data-latest --force
fi
```

`--force` тут допустимий лише тому, що це узгоджена модель `data-latest`:
кожен запуск змінює один її коміт. Перед виконанням push ще раз переконатися,
що немає активного GitHub Actions або іншого ручного запису.

Перевірити, що raw-посилання віддає новий файл:

```text
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/merged.csv
```

### 5. Закомітити `sku_map.json` та `image_cache.json` у `main`

Ці файли створюються/оновлюються під час роботи пауків і мають зберігатися в
`main`, а не в `data-latest`. Їх слід публікувати після CSV і до запуску
Prom-імпорту, як це робить workflow:

```bash
git add -- data/*/sku_map.json data/*/image_cache.json
if ! git diff --cached --quiet; then
  commit_time="$(date -u '+%Y-%m-%d %H:%M UTC')"
  git commit -m "chore: sku_map + image_cache update $commit_time"
  git pull --rebase origin main
  git push origin main
fi
```

Якщо `git pull --rebase` має конфлікт, зупинитися та розв'язати його вручну.
Не застосовувати force-push до `main` у ручному сценарії.

### 6. Запустити примусовий імпорт у Prom

У кабінеті Prom запустити імпорт за посиланням вище в режимі повного оновлення
та дочекатися саме статусу завершення в історії імпортів. Поки імпорт не
завершився успішно, не запускати noindex, ProSale або генератори фідів.

Запуск через UI не змінює `data/prom_import_status.json`. Не створювати цей
файл вручну. Якщо у ньому залишився попередній статус `failed`, наступний
автоматичний запуск безпечно відправить повну актуальну версію повторно через
`prom_merge_pending.py`. Не запускати `prom_api_trigger.py` після ручного
імпорту — це створить дубльований імпорт.

### 7. Виконати Prom-автоматизації

Після успішного імпорту та появи змін у публічному фіді Prom виконати:

```bash
python scripts/prom_noindex_automation.py
python scripts/prom_prosale_automation.py
```

Порядок навмисний: у фактичному workflow спочатку виконується noindex, а
потім ProSale. Для локального запуску скрипти використовують збережену сесію
в `pw-profile`; якщо сесія протермінована, увійти в Prom у відкритому браузері
та повторити конкретний скрипт.

### 8. Згенерувати й опублікувати Kasta, Epicenter та Rozetka XML

Генератори беруть актуальний публічний XML Prom і використовують локальні
`*_old.csv`. Для Rozetka також потрібен cache стоп-брендів із `data-latest`:

```bash
if [[ -f "$publish_dir/data/markets/rozetka_stop_brands_cache.json" ]]; then
  mkdir -p data/markets
  cp "$publish_dir/data/markets/rozetka_stop_brands_cache.json" data/markets/rozetka_stop_brands_cache.json
fi

python scripts/generate_kasta_feed.py
python scripts/generate_epicenter_feed.py
python scripts/generate_rozetka_feed.py

ls -lh data/markets/kasta_feed.xml data/markets/epicenter_feed.xml data/markets/rozetka_feed.xml
```

Перед публікацією ротувати `*_feed_prev.xml`, потім додати нові фіди та
оновлений cache Rozetka до того самого клону `data-latest`:

```bash
mkdir -p "$publish_dir/data/markets"
for market in kasta epicenter rozetka; do
  source_file="data/markets/${market}_feed.xml"
  current="$publish_dir/data/markets/${market}_feed.xml"
  previous="$publish_dir/data/markets/${market}_feed_prev.xml"
  if [[ -f "$current" ]]; then
    cp "$current" "$previous"
  fi
  cp "$source_file" "$current"
done
if [[ -f data/markets/rozetka_stop_brands_cache.json ]]; then
  cp data/markets/rozetka_stop_brands_cache.json "$publish_dir/data/markets/rozetka_stop_brands_cache.json"
fi

git -C "$publish_dir" add -- data/markets
if ! git -C "$publish_dir" diff --cached --quiet; then
  git -C "$publish_dir" commit --amend --no-edit
  git -C "$publish_dir" push origin data-latest --force
fi
```

Після успішного push перевірити raw-посилання:

```text
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/kasta_feed.xml
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/epicenter_feed.xml
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/rozetka_feed.xml
```

Потім запустити імпорт Epicenter. За потреби видимого браузера додати
`--headed`:

```bash
python scripts/epicenter_import_feed.py
```

### 9. Вечірній Merchant Center feed

Це незалежний вечірній етап (за розкладом — 21:00 Kyiv). Після того як
публічний Google Merchant XML Prom став актуальним, згенерувати фід:

```bash
python scripts/generate_merchant_feed.py
ls -lh data/markets/merchant_feed.xml
```

Для публікації взяти **свіжий** клон `data-latest` (попередній може бути
застарілим, якщо між етапами була інша публікація), ротувати попередній фід і
push через amend:

```bash
merchant_publish_dir="$(mktemp -d -t price-feed-pipeline-merchant.XXXXXX)"
git clone --branch data-latest --single-branch "$remote" "$merchant_publish_dir"

merchant_current="$merchant_publish_dir/data/markets/merchant_feed.xml"
merchant_previous="$merchant_publish_dir/data/markets/merchant_feed_prev.xml"
mkdir -p "$merchant_publish_dir/data/markets"
if [[ -f "$merchant_current" ]]; then
  cp "$merchant_current" "$merchant_previous"
fi
cp data/markets/merchant_feed.xml "$merchant_current"

git -C "$merchant_publish_dir" config user.name "manual-pipeline"
git -C "$merchant_publish_dir" config user.email "manual-pipeline@users.noreply.github.com"
git -C "$merchant_publish_dir" add -- data/markets
if ! git -C "$merchant_publish_dir" diff --cached --quiet; then
  git -C "$merchant_publish_dir" commit --amend --no-edit
  git -C "$merchant_publish_dir" push origin data-latest --force
fi
```

Фінальне посилання для Merchant Center:

```text
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/merchant_feed.xml
```
