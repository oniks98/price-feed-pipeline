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
    price_type = scrapy.Field()  # retail або dealer
    
    # Динамические характеристики (будут добавлены в паук)
    # Назва_Характеристики_1, Одиниця_виміру_Характеристики_1, Значення_Характеристики_1, ...


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
