"""
УЛЬТРА-ЧИСТИЙ запуск Scrapy з мультиканальним режимом

Використання:
  python scripts/ultra_clean_run.py <spider_name>

Правило іменування вихідного файлу:
  {supplier_name}_new.csv, де supplier_name = перша частина імені паука до '_'

Приклади:
  python scripts/ultra_clean_run.py viatec_dealer   -> data/output/viatec_new.csv
  python scripts/ultra_clean_run.py secur_retail    -> data/output/secur_new.csv
  python scripts/ultra_clean_run.py eserver_retail  -> data/output/eserver_new.csv
  python scripts/ultra_clean_run.py neolight_retail -> data/output/neolight_new.csv

Додавання нового постачальника:
  Достатньо створити паука з іменем {supplier}_{mode} — файл буде названо автоматично.
  Жодних змін у цьому скрипті не потрібно.
"""
import sys
import os
import warnings

from pathlib import Path


# КРИТИЧНО: Додаємо кореневу директорію проекту до sys.path
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Встановлюємо environment variables ДО всього
os.environ['SCRAPY_SETTINGS_MODULE'] = 'suppliers.settings'

# Ігноруємо DeprecationWarning
warnings.filterwarnings('ignore', category=DeprecationWarning)


# Патчимо configure_logging ДО імпорту Scrapy
def silent_configure_logging(settings=None, install_root_handler=True):
    """Наша версія configure_logging яка приховує технічні логи"""
    import logging

    # Базова конфігурація
    logging.basicConfig(
        format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
    )

    # Відключаємо всі технічні Scrapy логери
    noisy = [
        'scrapy.utils.log',
        'scrapy.addons',
        'scrapy.middleware',
        'scrapy.crawler',
        'scrapy.core.engine',
        'scrapy.core.scraper',
        'scrapy.extensions',
        'scrapy.statscollectors',
        'twisted',
        'filelock',
        'py.warnings',
    ]

    for name in noisy:
        logging.getLogger(name).setLevel(logging.ERROR)


# Патчимо Scrapy ДО імпорту
import scrapy.utils.log
scrapy.utils.log.configure_logging = silent_configure_logging

# Тепер імпортуємо решту
from scrapy.cmdline import execute


def get_output_filename(spider_name: str) -> str:
    """
    Визначає ім'я вихідного файлу для spider.

    Правило: supplier_name = перша частина імені паука до '_'
    Результат завжди: {supplier_name}_new.csv

    Приклади:
      viatec_dealer   -> viatec_new.csv
      viatec_retail   -> viatec_new.csv
      secur_retail    -> secur_new.csv
      eserver_retail  -> eserver_new.csv
      neolight_retail -> neolight_new.csv

    При додаванні нового постачальника цей файл змінювати не потрібно.
    """
    supplier_name = spider_name.split("_")[0]
    return f"{supplier_name}_new.csv"


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("❌ Використання: python scripts/ultra_clean_run.py <spider_name>")
        print("📝 Приклад: python scripts/ultra_clean_run.py eserver_retail")
        sys.exit(1)

    spider_name = sys.argv[1]
    output_file = get_output_filename(spider_name)

    print("\n" + "="*80)
    print(f"🚀 ЗАПУСК SPIDER: {spider_name}")
    print(f"📦 Режим: Мультиканальний")
    print(f"📄 Вихідний файл: data/output/{output_file}")
    print("="*80 + "\n")

    # Запускаємо spider (зберігаємо додаткові аргументи після spider_name)
    extra_args = sys.argv[2:]
    sys.argv = ['scrapy', 'crawl', spider_name] + extra_args

    try:
        execute()
        print("\n✅ Spider виконано успішно")
        print(f"📄 Результат збережено в: data/output/{output_file}")
        sys.exit(0)
    except SystemExit as e:
        if e.code == 0:
            print("\n✅ Spider виконано успішно")
            print(f"📄 Результат збережено в: data/output/{output_file}")
        else:
            print("\n❌ Spider завершився з помилками")
        sys.exit(e.code)
    except Exception as e:
        print(f"❌ ПОМИЛКА ПРИ ЗАПУСКУ SPIDER: {e}")
        sys.exit(1)
