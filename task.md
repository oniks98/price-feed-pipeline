случилась беда на гитакатион- не работал около 6 часов ( не было свободных виртуальных машин) и мой ран не запускался, при єтом гит хаб работал и ссілки раздавал. В принципе, не критично, но логично сделать запсаной вариант на будущее, если гит актион не будет рабоать , это маловероятно, конечно, но все же может быть.

Поєтому хочу чтобы ты написал пошаговый план , чтобы ран можно было сделать ручками.
Что имееем:
C:\FullStack\PriceFeedPipeline\.github\workflows\pipeline.yml
после него по расписанию ночью запускается
C:\FullStack\PriceFeedPipeline\.github\workflows\pipeline_merchant_feed.yml

как я вижу план ( ты откорректируй оптимально и логично)

1. Надо с гит -хаба сделать пуш chore: sku_map + image_cache update дата время в майн и получить для поставщиков последние
   sku_map.json
   image_cache.json
2. Запустить C:\FullStack\PriceFeedPipeline\scripts\products_copy_csvs_main.py - но вот тут вопрос : может сначала нужно перейти в data-latest сделать пуш и получить последние файлы old.csv и потом перейти в майн и потом уже запускать скрипт, чтобы получить именно с гит хаба с ветки data-latest последние версии
   3.Запускаем пауков - можно сделать скрипт , который будет запускать последовательно
   python scripts/ultra_clean_run.py viatec_dealer
   python scripts/ultra_clean_run.py secur_feed
   python scripts/ultra_clean_run.py lp_api
   python scripts/update_products.py
   C:\FullStack\PriceFeedPipeline\scripts\prom_merge_csv.py

получили
data/merged.csv

3. Теперь надо перенести все import_products.csv , old.csv, data/merged.csv ветку дата-латест и потом запушить на нит-хаб , чтобі он раздал по ссілке на пром merged.csv
4. На проме принудительно запущу импорт ссілки с гит-хаба , жду пока импорт закончится
5. Запускаю
   C:\FullStack\PriceFeedPipeline\scripts\prom_prosale_automation.py
   и потом
   C:\FullStack\PriceFeedPipeline\scripts\prom_noindex_automation.py

6. Запускаю генраторы
   C:\FullStack\PriceFeedPipeline\scripts\generate_kasta_feed.py
   C:\FullStack\PriceFeedPipeline\scripts\generate_epicenter_feed.py
   C:\FullStack\PriceFeedPipeline\scripts\generate_rozetka_feed.py
   получаю xml в майн - надо перекинуть в дата-латест и запушить на гитхаб
7. Запускаю C:\FullStack\PriceFeedPipeline\scripts\epicenter_import_feed.py

8. Вечером запускаю
   C:\FullStack\PriceFeedPipeline\scripts\generate_merchant_feed.py

Проверь внимательно и напиши план в конец файла C:\FullStack\PriceFeedPipeline\START.md
