# spider'и

python scripts/ultra_clean_run.py viatec_dealer
python scripts/ultra_clean_run.py viatec_retail

python scripts/ultra_clean_run.py secur_retail
python scripts/ultra_clean_run.py secur_feed
python scripts/ultra_clean_run.py secur_feed_full

python scripts/ultra_clean_run.py eserver_retail

python scripts/ultra_clean_run.py lun_retail
python scripts/ultra_clean_run.py neolight_retail

# Порівняння та оновлення товарів

python scripts/update_products.py viatec dealer
python scripts/update_products.py viatec retail
python scripts/update_products.py secur retail
python scripts/update_products.py eserver retail

# Всі постачальники (dealer + retail)

python scripts/update_products.py

# Злиття в merged.csv

python scripts/merge_csv.py

# Генерує фіди :

python scripts/generate_kasta_feed.py
python scripts/generate_epicenter_feed.py
python scripts/generate_rozetka_feed.py

https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/kasta_feed.xml
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/epicenter_feed.xml
https://raw.githubusercontent.com/oniks98/price-feed-pipeline/data-latest/data/markets/rozetka_feed.xml

# Додавання noindex

python scripts/prom_noindex_automation.py

# Додавання ProSale

python scripts/prom_prosale_automation.py

# Синхронізує категорії з фіду PROM з локальними файлами маркетплейсів

python scripts/export_prom_categories.py

# Витягує cookies сесії Prom і зберігає їх у prom_cookies.json

python scripts/export_prom_cookies.py

# Тригер імпорту товарів в Prom.ua через API після git push

python scripts/prom_api_trigger.py

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
