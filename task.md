Есть паук C:\FullStack\PriceFeedPipeline\suppliers\spiders\viatec\dealer.py и C:\FullStack\PriceFeedPipeline\suppliers\services\viatec_feed_service.py

Напиши нового паука
C:\FullStack\PriceFeedPipeline\suppliers\spiders\viatec\feed_full.py и C:\FullStack\PriceFeedPipeline\suppliers\services\viatec_feed_full_service.py
По такой логике

# НОВИЙ потік:

#

# 1. ViatecFeedService при старті: завантажує фід → будує 2 словники:

# {url: {name_ua, description_ua, image, available, params}}

# {sku: vendor} ← вже є

#

# 2. start() — тільки логін

#

# 3. after_login() — ітерація по feed_service.get_all_urls()

# замість category crawling

# → одразу yield Request(ru_url, callback=parse_product_ru)

# з meta {name_ua, description_ua, image} з фіду

#

# 4. parse_product_ru() — забирає зі сторінки:

# name_ru, description_ru, dealer_price, rrp, gallery, availability, quantity, specs

# + мерджить з ua-даними з meta

#

# Прибирається: parse_category, parse_product (UA), пагінація, \_load_category_mapping

constants.py — додати viatec_feed_full в два словники (pipeline підхопить автоматично)
Pipeline змінювати не треба — він data-driven через SupplierConfig.from_spider(spider_name).
