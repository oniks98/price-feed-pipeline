# spider'и

python scripts/ultra_clean_run.py viatec_dealer
python scripts/ultra_clean_run.py viatec_retail

python scripts/ultra_clean_run.py secur_retail
python scripts/ultra_clean_run.py secur_feed
python scripts/ultra_clean_run.py secur_feed_full

python scripts/ultra_clean_run.py eserver_retail

python scripts/ultra_clean_run.py lun_retail
python scripts/ultra_clean_run.py neolight_retail

# Оновлення товарів

python scripts/update_products.py viatec dealer
python scripts/update_products.py viatec retail
python scripts/update_products.py secur retail
python scripts/update_products.py eserver retail
python scripts/update_products.py lun retail
python scripts/update_products.py neolight retail

# Додавання noindex

python scripts/prom_noindex_automation.py

# Додавання ProSale

python scripts/prom_prosale_automation.py

# Всі постачальники (dealer + retail)

python scripts/update_products.py

# Злиття в merged.csv

python scripts/merge_csv.py

# Генерує фіди :

python scripts/generate_kasta_feed.py
python scripts/generate_epicenter_feed.py
python scripts/generate_rozetka_feed.py

https://raw.githubusercontent.com/oniks98/scrapy-suppliers/main/data/markets/kasta_feed.xml
https://raw.githubusercontent.com/oniks98/scrapy-suppliers/main/data/markets/epicenter_feed.xml
https://raw.githubusercontent.com/oniks98/scrapy-suppliers/main/data/markets/rozetka_feed.xml

# Скрипт оновлення товарів

Скрипт порівнює старий список товарів з вашого сайту з новим списком від постачальника та створює файл для імпорту з оновленими даними.

## 🎯 Логіка роботи

### 1. Товари, що існують в обох файлах:

- ✅ **Наявність ТА Кількість однакові** → НЕ додаємо в import (нема що оновлювати)
- 🔄 **Кількість різні** → Додаємо зі старого файлу з НОВОЮ Кількість
- 🔄 **Наявність різні** → Додаємо зі старого файлу з НОВОЮ Наявність
- 🔄 **Обидва параметри різні** → Додаємо зі старого з ОБОМА новими значеннями

### 2. Нові товари (є в new, немає в old):

- ➕ Додаються в КІНЕЦЬ файлу
- 🔢 Код товару = максимальний код зі старого файлу + 1, + 2, + 3...

### 3. Відсутні товари (є в old, немає в new):

- ⚠️ Додаються з Наявність = "-" та Кількість = "0" (позначає відсутність у постачальника)

## 📁 Структура файлів

```
C:\FullStack\Scrapy\
├── scripts\
│   └── update_products.py      ← Основний скрипт
├── run_update.bat              ← Швидкий запуск
├── data\
│   └── viatec\
│       ├── old_products.csv    ← Старий список (з вашого сайту)
│       ├── new_products.csv    ← Новий список (від постачальника)
│       └── import_products.csv ← РЕЗУЛЬТАТ (для імпорту)
```

## 📊 Приклад виводу

```
============================================================
🚀 СКРИПТ ОНОВЛЕННЯ ТОВАРІВ v1.2
============================================================

📁 Файли:
   Старий: C:\FullStack\Scrapy\data\viatec\old_products.csv
   Новий:  C:\FullStack\Scrapy\data\viatec\new_products.csv
   Вихід:  C:\FullStack\Scrapy\data\viatec\import_products.csv

✅ Прочитано 4 товарів з old_products.csv
   Колонок: 250
✅ Прочитано 4 товарів з new_products.csv
   Колонок: 250

📊 Статистика:
   Старих товарів: 4
   Нових товарів: 4

🔄 Обробка існуючих товарів...
➕ Обробка нових товарів...

💾 Запис результатів...
✅ Файл успішно створено!

============================================================
📈 ПІДСУМКОВА СТАТИСТИКА:
============================================================
  Без змін (не додано):           1
  Змінилася кількість:            1
  Змінилася наявність:            1
  Змінилося обидва параметри:     0
  Відсутні в новому файлі:        1
  Нові товари:                    1
------------------------------------------------------------
  ВСЬОГО для імпорту:             4
============================================================

✅ Готово!
```

# Видаляємо коміт бота з remote — remote стане = локальному HEAD

Локальний код не закомічений, тому він у повній безпеці.
git push --force не чіпає незакомічені файли взагалі.

git push origin HEAD:main --force

# Ховаємо зміни

git stash → прибере локальні зміни
git pull --rebase
→ підтягне коміти з GitHub
→ без merge-комміту
git stash pop → поверне твої зміни

# Видалити локальний коміт без втрати змін

git reset --soft origin/main
усі зміни як незакоммічені
без конфліктів історії
потім пул з репо і знову коміт локально

# Сортування минус-слів у гугл таблиці

=SORT(UNIQUE(TOCOL(SPLIT(A:A;" ");1)))
=SORT(UNIQUE(TOCOL(SPLIT(REGEXREPLACE(A:A;"[!@%,*]";"");" ");1)))
=SORT(UNIQUE(TOCOL(SPLIT(REGEXREPLACE(A:A;"[!@%,*\.\(\)\[\]]";"");" ");1)))
