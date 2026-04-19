необходимо сделать рефактор
C:\FullStack\PriceFeedPipeline\suppliers\pipelines.py
C:\FullStack\PriceFeedPipeline\suppliers\spiders\viatec\dealer.py
C:\FullStack\PriceFeedPipeline\scripts\merge_csv.py

что сейчас имеем:
паук дает цену РРЦ и дилерскую и пишем ее соответсвенно по файлу C:\FullStack\PriceFeedPipeline\data\viatec\viatec_category.csv по каналам prom и site.

необходимо:
1.паук парсит текущий курс на сайте поставщика по тегу

<p class="lk-nav__admin-bottom-dollar-usd bold column-gap-1">
                                    <span class="lk-nav__admin-bottom-dollar-usd-name">USD б/г</span>
                                    <span class="lk-nav__admin-bottom-dollar-usd-value text-right">44.00</span>
                                </p>
необходимо также дефолтное значение, например 43.8, на случай поставить ручками, если паук не спарсит.
2. Конвертирует цену dealer умножая на спарсенный курс (или дефолт) и пайплайн записывает значение в импортный файл столбик "Оптова_ціна"- єто будет цена dealer.
3. Затем сделать где логично - в пауке или пайплайне (вероятно дополнительный сервис в C:\FullStack\PriceFeedPipeline\suppliers\services  чтоб не мусорить в пайплайне) необходимо сделать сравнение:
1 условие
если retail(РРЦ)/dealer больше или равно 1,35  , 
то тогда пишем  retail(РРЦ) в валюте UAH в канал prom в импортный файл столбик "Ціна"
и умножаем на коэффициент в C:\FullStack\PriceFeedPipeline\data\viatec\viatec_category.csv  из столбика coef_retail (дефолт число 1) по єтой категории
2 условие
 а если меньше 1,35 то тогда пишем dealer в валюте UAH в канал prom в импортный файл столбик "Ціна"
 и умножаем на коэффициент в C:\FullStack\PriceFeedPipeline\data\viatec\viatec_category.csv  из столбика coef_dealer (дефолт число 1,2) по єтой категории
 3 условие
 канал site заполняем только ценой dealer в валюте UAH в импортный файл столбик "Ціна"
 и умножаем на коэффициент в C:\FullStack\PriceFeedPipeline\data\viatec\viatec_category.csv  из столбика coef_dealer (дефолт число 1,2) по єтой категории

4.в C:\FullStack\PriceFeedPipeline\scripts\merge*csv.py сделать очистку в merge_csv столбика "Оптова*ціна"

Если тебе что-то не понятно, то уточни до рефактора и только потом пиши код
