Вивчи структуру правил у файлі
C:\FullStack\Scrapy\data\viatec\viatec_mapping_rules – domofony.csv
як приклад формату та логіки.

Проаналізуй ДВА файли:

1. C:\FullStack\Scrapy\data\viatec\dictionary_attribute\kommutatory_prom_attribute.csv

- портальні характеристики Prom (це ЄДИНИЙ список дозволених prom_attribute)

2. C:\FullStack\Scrapy\data\viatec\dictionary_attribute\kommutatory_supplier_attribute.csv

- характеристики постачальника (це ЄДИНИЙ список дозволених supplier_attribute)

АЛГОРИТМ РОБОТИ:
КРОК 1: Прочитай файл *prom_attribute.csv та створи ПОВНИЙ список всіх назв характеристик з колонки "Назва*Характеристики". Це твій WHITE LIST для prom*attribute.
КРОК 2: Прочитай файл \_supplier_attribute.csv та створи ПОВНИЙ список всіх назв характеристик з колонок "Назва*Характеристики". Це твій WHITE LIST для supplier_attribute.
КРОК 3: Для кожного правила маппінгу ОБОВ'ЯЗКОВО перевір:

- Чи є supplier_attribute в WHITE LIST з кроку 2?
- Чи є prom_attribute в WHITE LIST з кроку 1?
- Якщо БУДЬ-ЯКЕ з цих полів відсутнє в WHITE LIST - НЕ створюй це правило!
  КРОК 4: Створи правила ТІЛЬКИ для тих характеристик, які пройшли перевірку в кроці 3.

АБСОЛЮТНІ ЗАБОРОНИ:
❌ ЗАБОРОНЕНО використовувати prom*attribute, яких немає в колонці "Назва*Характеристики" файлу *prom_attribute.csv
❌ ЗАБОРОНЕНО використовувати supplier_attribute, яких немає в колонках "Назва*Характеристики" файлу \_supplier_attribute.csv
❌ ЗАБОРОНЕНО вигадувати, припускати або додавати характеристики, яких фізично немає у файлах
❌ ЗАБОРОНЕНО створювати правила "на всяк випадок" або "можливо існують"

ДОЗВОЛЕНО:
✅ Витягувати інформацію з назв товарів (supplier_name_substring) - це поле може бути порожнім у supplier_attribute
✅ Створювати кілька правил для однієї характеристики з різними патернами
✅ Використовувати regex для екстракції значень

ПЕРЕД СТВОРЕННЯМ ФАЙЛУ:
Виведи список всіх prom_attribute, які ти знайшов у файлі \_prom_attribute.csv
Виведи список всіх supplier_attribute, які ти знайшов у файлі \_supplier_attribute.csv
Поясни, які правила ти створиш та чому

Створи новий файл C:\FullStack\Scrapy\data\viatec\viatec_mapping_rules–kommutatory.csv з правилами маппінгу.
