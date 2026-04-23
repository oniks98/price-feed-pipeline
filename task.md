необходимо сделать рефактор
C:\FullStack\PriceFeedPipeline\scripts\generate_epicenter_feed.py
C:\FullStack\PriceFeedPipeline\scripts\generate_kasta_feed.py
C:\FullStack\PriceFeedPipeline\scripts\generate_rozetka_feed.py

сейчас алгоритм такой по цене:
фид прома-цена из фида-умножение цены на коєф из C:\FullStack\PriceFeedPipeline\data\markets\markets_coefficients.csv по category_id или по дефолту- получаем фид с новой ценой для маркетплейса.

необходимо немного изменить умножение цены:
фид прома-берем из него <article>число</article>- смотрим article-число в ветке дата латест в папке data\viatec\viatec*old.csv и data\secur\secur_old.csv в столбике Код*товару - выбираем тот article у которого в столбике Ідентифікатор*товару
число БЕЗ префикса prom* - по этому Ідентифікатор*товару БЕЗ префикса prom* находим соответсвенно цену в столбике Оптова*ціна - умножаем эту цену на коэф согласно category_id из C:\FullStack\PriceFeedPipeline\data\markets\markets_coefficients.csv или по дефолту, если нет Ідентифікатор*товару БЕЗ префикса prom* или цены в столбике Оптова*ціна- получаем фид с новой ценой для маркетплейса
