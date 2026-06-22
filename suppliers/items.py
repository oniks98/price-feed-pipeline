import scrapy


class ViatecProductItem(scrapy.Item):
    """Item для товаров с сайта viatec.ua"""
    
    # Основные поля
    Код_товару = scrapy.Field()
    Назва_позиції = scrapy.Field()
    Назва_позиції_укр = scrapy.Field()
    Пошукові_запити = scrapy.Field()
    Пошукові_запити_укр = scrapy.Field()
    Опис = scrapy.Field()
    Опис_укр = scrapy.Field()
    Тип_товару = scrapy.Field()
    Ціна = scrapy.Field()
    Валюта = scrapy.Field()
    Одиниця_виміру = scrapy.Field()
    Мінімальний_обсяг_замовлення = scrapy.Field()
    Оптова_ціна = scrapy.Field()
    Мінімальне_замовлення_опт = scrapy.Field()
    Посилання_зображення = scrapy.Field()
    Наявність = scrapy.Field()
    Кількість = scrapy.Field()
    Номер_групи = scrapy.Field()
    Назва_групи = scrapy.Field()
    Назва_групи_укр = scrapy.Field()
    Посилання_підрозділу = scrapy.Field()
    Можливість_поставки = scrapy.Field()
    Термін_поставки = scrapy.Field()
    Спосіб_пакування = scrapy.Field()
    Спосіб_пакування_укр = scrapy.Field()
    Унікальний_ідентифікатор = scrapy.Field()
    Ідентифікатор_товару = scrapy.Field()
    Ідентифікатор_підрозділу = scrapy.Field()
    Ідентифікатор_групи = scrapy.Field()
    Виробник = scrapy.Field()
    Країна_виробник = scrapy.Field()
    Знижка = scrapy.Field()
    ID_групи_різновидів = scrapy.Field()
    Особисті_нотатки = scrapy.Field()
    Продукт_на_сайті = scrapy.Field()
    Термін_дії_знижки_від = scrapy.Field()
    Термін_дії_знижки_до = scrapy.Field()
    Ціна_від = scrapy.Field()
    Ярлик = scrapy.Field()
    HTML_заголовок = scrapy.Field()
    HTML_заголовок_укр = scrapy.Field()
    HTML_опис = scrapy.Field()
    HTML_опис_укр = scrapy.Field()
    Код_маркування_GTIN = scrapy.Field()
    Номер_пристрою_MPN = scrapy.Field()
    Вага_кг = scrapy.Field()
    Ширина_см = scrapy.Field()
    Висота_см = scrapy.Field()
    Довжина_см = scrapy.Field()
    Де_знаходиться_товар = scrapy.Field()
    
    # Технические поля (не экспортируются)
    category_url = scrapy.Field()
    price_type = scrapy.Field()      # retail або dealer
    supplier_id = scrapy.Field()     # viatec
    output_file = scrapy.Field()     # имя выходного файла
    specifications_list = scrapy.Field()  # список характеристик
    price_rrp_uah = scrapy.Field()   # РРЦ в гривнях зі сторінки
    usd_rate = scrapy.Field()        # курс USD на момент парсингу

    # Динамические характеристики (будут добавлены в pipeline)
    # Назва_Характеристики_1, Одиниця_виміру_Характеристики_1, Значення_Характеристики_1, ...


class LpProductItem(scrapy.Item):
    """Item для товарів LogicPower B2B API (lp_api spider).

    Паук повертає плаский dict, але pipeline приймає і dict, і Item без різниці.
    Цей клас визначає канонічну схему всіх полів LP-постачальника.
    """

    # ──── Ідентифікація ─────────────────────────────────────────────
    Код_товару = scrapy.Field()             # призначається SkuCodeService
    Ідентифікатор_товару = scrapy.Field()   # код з API (product.code)

    # ──── Назва та опис ───────────────────────────────────────────
    Назва_позиції = scrapy.Field()
    Назва_позиції_укр = scrapy.Field()
    Опис = scrapy.Field()
    Опис_укр = scrapy.Field()
    Тип_товару = scrapy.Field()
    Одиниця_виміру = scrapy.Field()

    # ──── Ціна ───────────────────────────────────────────────────
    Ціна = scrapy.Field()                    # USD (personal dealer price)
    Валюта = scrapy.Field()                  # завжди "USD"
    Оптова_ціна = scrapy.Field()          # заповнюється pipeline (dealer_uah)
    Мінімальний_обсяг_замовлення = scrapy.Field()
    Мінімальне_замовлення_опт = scrapy.Field()

    # ──── Наявність ───────────────────────────────────────────────
    Наявність = scrapy.Field()
    Кількість = scrapy.Field()

    # ──── Категорія / канал (заповнює ChannelService) ─────────────
    Номер_групи = scrapy.Field()
    Назва_групи = scrapy.Field()
    Назва_групи_укр = scrapy.Field()
    Ідентифікатор_підрозділу = scrapy.Field()
    Ідентифікатор_групи = scrapy.Field()
    Посилання_підрозділу = scrapy.Field()
    Особисті_нотатки = scrapy.Field()
    Ярлик = scrapy.Field()

    # ──── Виробник / медіа ──────────────────────────────────────
    Виробник = scrapy.Field()
    Країна_виробник = scrapy.Field()
    Посилання_зображення = scrapy.Field()
    Продукт_на_сайті = scrapy.Field()     # externalUrl

    # ──── Пошукові запити ──────────────────────────────────────
    Пошукові_запити = scrapy.Field()
    Пошукові_запити_укр = scrapy.Field()

    # ──── Цінові дані зі стандартних полів Prom ───────────────────
    Знижка = scrapy.Field()
    Термін_дії_знижки_від = scrapy.Field()
    Термін_дії_знижки_до = scrapy.Field()
    Ціна_від = scrapy.Field()
    Редагування = scrapy.Field()
    ID_групи_різновидів = scrapy.Field()
    Можливість_поставки = scrapy.Field()
    Термін_поставки = scrapy.Field()
    Спосіб_пакування = scrapy.Field()
    Спосіб_пакування_укр = scrapy.Field()
    Унікальний_ідентифікатор = scrapy.Field()
    Вага_кг = scrapy.Field()
    Ширина_см = scrapy.Field()
    Висота_см = scrapy.Field()
    Довжина_см = scrapy.Field()
    HTML_заголовок = scrapy.Field()
    HTML_заголовок_укр = scrapy.Field()
    HTML_опис = scrapy.Field()
    HTML_опис_укр = scrapy.Field()
    Код_маркування_GTIN = scrapy.Field()
    Номер_пристрою_MPN = scrapy.Field()
    Де_знаходиться_товар = scrapy.Field()

    # ──── Технічні поля (pipeline, не потрапляють в CSV) ──────────────
    supplier_id = scrapy.Field()         # "lp"
    output_file = scrapy.Field()         # "lp_new.csv"
    source = scrapy.Field()              # "api"
    price_type = scrapy.Field()          # "dealer"
    usd_rate = scrapy.Field()            # курс USD (businessEntity)
    price_rrp_uah = scrapy.Field()       # recommendedRetail UAH
    category_url = scrapy.Field()        # пустий для LP (використовує category_id)
    category_id = scrapy.Field()         # код категорії з lp_category.csv
    feed_id = scrapy.Field()             # для фідів (поки не використовується)
    specifications_list = scrapy.Field() # list[{name, unit, value}]

    # Динамічні характеристики (додає pipeline)
    # Назва_Характеристики_1, Одиниця_виміру_Характеристики_1, Значення_Характеристики_1 ...


class EserverProductItem(scrapy.Item):
    """Item для товаров с сайта e-server.com.ua"""
    
    # Основные поля
    Код_товару = scrapy.Field()
    Назва_позиції = scrapy.Field()
    Назва_позиції_укр = scrapy.Field()
    Пошукові_запити = scrapy.Field()
    Пошукові_запити_укр = scrapy.Field()
    Опис = scrapy.Field()
    Опис_укр = scrapy.Field()
    Тип_товару = scrapy.Field()
    Ціна = scrapy.Field()
    Валюта = scrapy.Field()
    Одиниця_виміру = scrapy.Field()
    Мінімальний_обсяг_замовлення = scrapy.Field()
    Оптова_ціна = scrapy.Field()
    Мінімальне_замовлення_опт = scrapy.Field()
    Посилання_зображення = scrapy.Field()
    Наявність = scrapy.Field()
    Кількість = scrapy.Field()
    Номер_групи = scrapy.Field()
    Назва_групи = scrapy.Field()
    Назва_групи_укр = scrapy.Field()
    Посилання_підрозділу = scrapy.Field()
    Можливість_поставки = scrapy.Field()
    Термін_поставки = scrapy.Field()
    Спосіб_пакування = scrapy.Field()
    Спосіб_пакування_укр = scrapy.Field()
    Унікальний_ідентифікатор = scrapy.Field()
    Ідентифікатор_товару = scrapy.Field()
    Ідентифікатор_підрозділу = scrapy.Field()
    Ідентифікатор_групи = scrapy.Field()
    Виробник = scrapy.Field()
    Країна_виробник = scrapy.Field()
    Знижка = scrapy.Field()
    ID_групи_різновидів = scrapy.Field()
    Особисті_нотатки = scrapy.Field()
    Продукт_на_сайті = scrapy.Field()
    Термін_дії_знижки_від = scrapy.Field()
    Термін_дії_знижки_до = scrapy.Field()
    Ціна_від = scrapy.Field()
    Ярлик = scrapy.Field()
    HTML_заголовок = scrapy.Field()
    HTML_заголовок_укр = scrapy.Field()
    HTML_опис = scrapy.Field()
    HTML_опис_укр = scrapy.Field()
    Код_маркування_GTIN = scrapy.Field()
    Номер_пристрою_MPN = scrapy.Field()
    Вага_кг = scrapy.Field()
    Ширина_см = scrapy.Field()
    Висота_см = scrapy.Field()
    Довжина_см = scrapy.Field()
    Де_знаходиться_товар = scrapy.Field()
    
    # Технические поля (не экспортируються)
    price_type = scrapy.Field()  # retail або dealer
    supplier_id = scrapy.Field()  # eserver
    output_file = scrapy.Field()  # имя выходного файла
    specifications_list = scrapy.Field()  # список характеристик
    category_url = scrapy.Field()  # URL категории для мультиканального режиму
    
    # Динамические характеристики (будут добавлены в pipeline)
    # Назва_Характеристики_1, Одиниця_виміру_Характеристики_1, Значення_Характеристики_1, ...


class SecurProductItem(scrapy.Item):
    """Item для товаров с сайта secur.ua"""
    
    # Основные поля
    Код_товару = scrapy.Field()
    Назва_позиції = scrapy.Field()
    Назва_позиції_укр = scrapy.Field()
    Пошукові_запити = scrapy.Field()
    Пошукові_запити_укр = scrapy.Field()
    Опис = scrapy.Field()
    Опис_укр = scrapy.Field()
    Тип_товару = scrapy.Field()
    Ціна = scrapy.Field()
    Валюта = scrapy.Field()
    Одиниця_виміру = scrapy.Field()
    Мінімальний_обсяг_замовлення = scrapy.Field()
    Оптова_ціна = scrapy.Field()
    Мінімальне_замовлення_опт = scrapy.Field()
    Посилання_зображення = scrapy.Field()
    Наявність = scrapy.Field()
    Кількість = scrapy.Field()
    Номер_групи = scrapy.Field()
    Назва_групи = scrapy.Field()
    Назва_групи_укр = scrapy.Field()
    Посилання_підрозділу = scrapy.Field()
    Можливість_поставки = scrapy.Field()
    Термін_поставки = scrapy.Field()
    Спосіб_пакування = scrapy.Field()
    Спосіб_пакування_укр = scrapy.Field()
    Унікальний_ідентифікатор = scrapy.Field()
    Ідентифікатор_товару = scrapy.Field()
    Ідентифікатор_підрозділу = scrapy.Field()
    Ідентифікатор_групи = scrapy.Field()
    Виробник = scrapy.Field()
    Країна_виробник = scrapy.Field()
    Знижка = scrapy.Field()
    ID_групи_різновидів = scrapy.Field()
    Особисті_нотатки = scrapy.Field()
    Продукт_на_сайті = scrapy.Field()
    Термін_дії_знижки_від = scrapy.Field()
    Термін_дії_знижки_до = scrapy.Field()
    Ціна_від = scrapy.Field()
    Ярлик = scrapy.Field()
    HTML_заголовок = scrapy.Field()
    HTML_заголовок_укр = scrapy.Field()
    HTML_опис = scrapy.Field()
    HTML_опис_укр = scrapy.Field()
    Код_маркування_GTIN = scrapy.Field()
    Номер_пристрою_MPN = scrapy.Field()
    Вага_кг = scrapy.Field()
    Ширина_см = scrapy.Field()
    Висота_см = scrapy.Field()
    Довжина_см = scrapy.Field()
    Де_знаходиться_товар = scrapy.Field()
    
    # Технические поля (не экспортируются)
    price_type = scrapy.Field()  # retail або dealer
    supplier_id = scrapy.Field()  # secur
    output_file = scrapy.Field()  # имя выходного файла
    specifications_list = scrapy.Field()  # список характеристик
    category_url = scrapy.Field()  # URL категории
    
    # Динамические характеристики (будут добавлены в pipeline)
    # Назва_Характеристики_1, Одиниця_виміру_Характеристики_1, Значення_Характеристики_1, ...
