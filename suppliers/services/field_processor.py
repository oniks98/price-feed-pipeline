"""
Сервіс для постобробки полів після застосування правил маппінгу.

Виконує конвертації одиниць виміру:
- Вага: SMART конвертація based on category_dealer.csv
- Габарити: мм → см (1500 мм → 150 см)
- HDD: SATA Тб → ГБ (4 SATA 8 Тб → 32768 ГБ)
- Батарея: А·г → мА·г (2.5 А·г → 2500 мА·г)
- Навантаження: г → кг або кг/м (залежно від типу)

ВАЖЛИВО: Методи отримують значення ВЖЕ ПІСЛЯ застосування правил маппінгу,
тобто в форматі "300 г" або "1.5 кг" (число + одиниця).
"""

import re
import csv
from pathlib import Path

from .validation_service import ValidationService


class FieldProcessor:
    """Постобробка полів для конвертації одиниць виміру"""
    
    def __init__(self, category_config_path: str | Path = None):
        """
        Ініціалізація з опціональним завантаженням конфігурації категорій.
        
        Args:
            category_config_path: шлях до viatec_category.csv
        """
        self.category_weight_units = {}  # {category_id: 'г' or 'кг'}
        
        if category_config_path:
            self._load_category_config(category_config_path)
    
    def _load_category_config(self, path: str | Path) -> None:
        """
        Завантажує конфігурацію одиниць виміру для категорій.

        ПРИЧИНА використання csv.reader замість csv.DictReader:
        viatec_category.csv містить дублікати заголовків:
          col 12: Назва_Характеристики             → "Вага"
          col 13: Одиниця_виміру_Характеристики → "г"/"кг"  ← нам потрібна ця
          col 16: Одиниця_виміру_Характеристики → ""         ← DictReader брав цю
        DictReader при дублікатах зберігає ОСТАННЄ значення → завжди "".
        Тому читаємо raw-рядки і звертаємося до колонок за індексом.

        Структура колонок viatec_category.csv (0-based):
          0:  №
          1:  Линк категории поставщика
          2:  channel
          8:  Ідентифікатор_підрозділу
          12: Назва_Характеристики                 ← лише "Вага"
          13: Одиниця_виміру_Характеристики  ← одиниця ваги ("г"/"кг")

        Один і той самий Ідентифікатор_підрозділу може бути в кількох
        supplier-категоріях, але портал має для нього одну одиницю ваги.
        Конфлікт у CSV є критичною помилкою конфігурації: не дозволяємо
        останньому рядку непомітно перезаписати коректне значення.
        """
        IDX_CHANNEL     = 2
        IDX_CAT_ID      = 8
        IDX_WEIGHT_NAME = 12
        IDX_WEIGHT_UNIT = 13
        WEIGHT_NAMES = frozenset({"вага", "вес", "weight"})
        WEIGHT_UNITS = frozenset({"г", "кг"})

        path = Path(path)
        try:
            configured_units: dict[str, str] = {}
            source_lines: dict[str, int] = {}
            with open(path, encoding="utf-8-sig") as f:
                reader = csv.reader(f, delimiter=";")
                headers = next(reader, None)
                if headers is None:
                    return

                for row_number, row in enumerate(reader, start=2):
                    if not row or all(c.strip() == "" for c in row):
                        continue

                    # Беремо тільки site-рядки, щоб не дублювати prom-рядки
                    channel = row[IDX_CHANNEL].strip() if len(row) > IDX_CHANNEL else ""
                    if channel != "site":
                        continue

                    category_id = row[IDX_CAT_ID].strip() if len(row) > IDX_CAT_ID else ""
                    weight_name = row[IDX_WEIGHT_NAME].strip().casefold() if len(row) > IDX_WEIGHT_NAME else ""
                    weight_unit = row[IDX_WEIGHT_UNIT].strip().casefold() if len(row) > IDX_WEIGHT_UNIT else ""

                    if weight_name not in WEIGHT_NAMES or weight_unit not in WEIGHT_UNITS:
                        continue

                    if not category_id:
                        continue

                    previous_unit = configured_units.get(category_id)
                    if previous_unit is not None and previous_unit != weight_unit:
                        previous_line = source_lines[category_id]
                        raise ValueError(
                            "Конфлікт одиниць ваги в category config: "
                            f"підрозділ={category_id}, рядки {previous_line} ({previous_unit}) "
                            f"і {row_number} ({weight_unit})"
                        )

                    configured_units[category_id] = weight_unit
                    source_lines[category_id] = row_number

            self.category_weight_units = configured_units

            g_cats  = [k for k, v in self.category_weight_units.items() if v == "г"]
            kg_cats = [k for k, v in self.category_weight_units.items() if v == "кг"]

            print(f"✅ Завантажено {len(self.category_weight_units)} категорій з {path.name}")
            print(
                f"   Категорії з 'г': {g_cats[:10]}..."
                if len(g_cats) > 10 else f"   Категорії з 'г': {g_cats}"
            )
            print(
                f"   Категорії з 'кг': {kg_cats[:10]}..."
                if len(kg_cats) > 10 else f"   Категорії з 'кг': {kg_cats}"
            )

        except FileNotFoundError:
            print(f"⚠️ FieldProcessor: CSV файл не знайдено: {path}")
        except ValueError:
            raise
        except Exception as e:
            print(f"❌ FieldProcessor: Помилка завантаження category_config: {e}")

    def process_weight(self, value: str, category_id: str, spider) -> str:
        """
        SMART конвертація ваги на основі вимог PROM для категорії.
        
        Логіка:
        - Якщо категорія вимагає "г" (грами):
            "300 г" → "300" (PROM додасть "г")
            "1.5 кг" → "1500" (конвертуємо в грами)
        
        - Якщо категорія вимагає "кг" (кілограми):
            "300 г" → "0,3" (конвертуємо в кг)
            "1.5 кг" → "1,5" (PROM додасть "кг")
        
        Args:
            value: "300 г" або "1.5 кг" (після маппінгу)
            category_id: PROM категорія (301105, 5280501 тощо)
            spider: для логування
        
        Returns:
            Число для PROM (без одиниць), десятковий розділювач — кома
        """
        if not value:
            return ""
        
        value = value.strip()
        
        # Визначаємо що вимагає PROM для цієї категорії
        required_unit = self.category_weight_units.get(category_id, 'г')  # За замовчуванням "г"
        
        # 🔍 DEBUG: Логування конфігурації
        spider.logger.info(f"🔍 WEIGHT DEBUG: category_id={category_id}, value='{value}', required_unit='{required_unit}'")
        spider.logger.info(f"🔍 Available units: {list(self.category_weight_units.items())[:5]}...")
        
        # Витягуємо число і одиницю з value
        match_g = re.match(r'([0-9]+(?:[.,][0-9]+)?)\s*г$', value)
        match_kg = re.match(r'([0-9]+(?:[.,][0-9]+)?)\s*кг$', value)
        
        if match_g:
            grams = float(match_g.group(1).replace(',', '.'))
            
            if required_unit == 'г':
                # PROM вимагає грами - залишаємо як є
                # decimal_sep='.' — Prom.ua приймає тільки крапку у фізичних полях
                # decimals=2 — max 2 знаки після крапки (захист від плутанини з роздільником тисяч)
                result = ValidationService.sanitize_prom_numeric(str(grams), decimals=2, decimal_sep='.')
                spider.logger.debug(f"⚖️ Вага (cat={category_id}, unit=г): {value} → {result} г")
                return result
            else:
                # PROM вимагає кг - конвертуємо г → кг
                kg = grams / 1000
                result = ValidationService.sanitize_prom_numeric(str(kg), decimals=2, decimal_sep='.')
                spider.logger.debug(f"⚖️ Вага (cat={category_id}, unit=кг): {value} → {result} кг")
                return result
        
        elif match_kg:
            kg = float(match_kg.group(1).replace(',', '.'))
            
            if required_unit == 'г':
                # PROM вимагає грами - конвертуємо кг → г
                grams = kg * 1000
                result = ValidationService.sanitize_prom_numeric(str(grams), decimals=2, decimal_sep='.')
                spider.logger.debug(f"⚖️ Вага (cat={category_id}, unit=г): {value} → {result} г")
                return result
            else:
                # PROM вимагає кг - залишаємо як є
                result = ValidationService.sanitize_prom_numeric(str(kg), decimals=2, decimal_sep='.')
                spider.logger.debug(f"⚖️ Вага (cat={category_id}, unit=кг): {value} → {result} кг")
                return result
        
        # Якщо формат незрозумілий - повертаємо як є
        spider.logger.warning(f"⚠️ Незрозумілий формат ваги: {value}")
        return value

    @staticmethod
    def process_dimension(value: str, field_name: str, spider) -> str:
        """
        Конвертація розмірів в сантиметри.
        
        Вхід (після маппінгу):
        - "1500 мм" → "150" (см)
        - "15 см" → "15" (см)
        
        Вихід: число в см (формат PROM з комою: "150,0")
        """
        if not value:
            return ""
        
        value = value.strip()
        
        # Розмір в міліметрах: "1500 мм" → 150 см
        if value.endswith(' мм'):
            try:
                mm = float(value.replace(' мм', '').replace(',', '.'))
                cm = mm / 10
                # Крапка як розділювач — Prom.ua приймає тільки крапку в цих полях
                # 1 знак після крапки — безпечно (роздільник тисяч завжди має рівно 3 цифри)
                result = f"{cm:.1f}"
                spider.logger.debug(f"📏 {field_name}: {value} → {result} см")
                return result
            except ValueError:
                spider.logger.warning(f"⚠️ Помилка конвертації {field_name}: {value}")
                return value
        
        # Розмір в сантиметрах: "15 см" → 15 см
        elif value.endswith(' см'):
            try:
                cm = float(value.replace(' см', '').replace(',', '.'))
                result = f"{cm:.1f}"
                spider.logger.debug(f"📏 {field_name}: {value} → {result} см")
                return result
            except ValueError:
                spider.logger.warning(f"⚠️ Помилка конвертації {field_name}: {value}")
                return value
        
        # Якщо одиниць немає - залишаємо як є
        return value

    @staticmethod
    def _preserve_gross_weight(spec: dict, spider) -> None:
        """
        «Вага брутто»/«Вес брутто»/«gross weight»: значення зберігається
        AS-IS (без конвертації в required_unit категорії), одиниця
        переноситься в назву характеристики.

        Визначення вихідної одиниці — той самий пріоритет, що і для
        звичайної ваги нижче: суфікс у value ("0.8 кг") → поле unit.
        Формат незрозумілий → spec лишається raw pass-through, як прийшов
        від постачальника.

        "0.8 кг" (value), unit=""  → name="... (кг)", value="0.8",  unit=""
        "400 г"  (value), unit=""  → name="... (г)",  value="400",  unit=""
        "0.8"    (value), unit="кг" → name="... (кг)", value="0.8",  unit=""
        """
        raw_value = spec.get('value', '').strip()
        raw_unit = spec.get('unit', '').strip().lower()
        raw_name = spec.get('name', '').strip()

        if raw_value.endswith(' кг'):
            source_unit = 'кг'
            numeric_part = raw_value[:-3].strip()
        elif raw_value.endswith(' г'):
            source_unit = 'г'
            numeric_part = raw_value[:-2].strip()
        elif raw_unit in ('г', 'кг'):
            source_unit = raw_unit
            numeric_part = raw_value
        else:
            spider.logger.warning(
                f"⚠️ Незрозумілий формат брутто-ваги: value={raw_value!r} unit={raw_unit!r}"
            )
            return

        normalized = ValidationService.sanitize_prom_numeric(numeric_part, decimals=3, decimal_sep='.')
        if not normalized:
            return

        spec['name'] = f"{raw_name} ({source_unit})"
        spec['value'] = normalized
        spec['unit'] = ''

        spider.logger.debug(
            f"⚖️ Брутто-вага збережена як є: {raw_name!r} → {spec['name']!r} = {spec['value']}"
        )

    def process_specs_weight(self, specs_list: list, category_id: str, spider) -> list:
        """
        SMART постобробка ваги в характеристиках.

        Використовує конфігурацію категорії для визначення одиниць.
        - Категорія вимагає 'г': "300 г" → "300" + unit="г"
        - Категорія вимагає 'кг': "300 г" → "0,3" + unit="кг"

        ВАЖЛИВО: значення може прийти в ОДНОМУ з трьох форматів:
        - єдиним рядком з одиницею всередині: value="300 г", unit=""
        - вже розділеним павуком через _SPEC_UNIT_RE: value="300", unit="г"
        - з одиницею, "запеченою" в саму назву характеристики постачальником:
          name="Вага, кг", value="753.775", unit="" (без суфікса у value)
        Раніше перевірявся лише перший формат (original_value.endswith(' г')),
        тому розділені specs і specs з одиницею в назві пролітали повз
        конвертацію і потрапляли в CSV необробленими (з крапкою замість
        коми). Тепер одиниця визначається з spec['unit'], а якщо вона
        порожня — fallback на суфікс у value, а потім на суфікс у назві.
        """
        if not specs_list:
            return specs_list

        # Визначаємо вимоги PROM для цієї категорії
        required_unit = self.category_weight_units.get(category_id, 'г')

        weight_names = [
            'вага', 'вага брутто', 'вага нетто',
            'weight', 'gross weight', 'net weight'
        ]
        # «Брутто»/gross-вага винесена в окрему гілку нижче (_preserve_gross_weight) —
        # тут лишається лише для довідки, фактично перехоплюється раніше циклу.
        gross_weight_names = ['вага брутто', 'вес брутто', 'gross weight']

        # Канонічні (без коми в назві) Prom-характеристики "Вага", які вже
        # присутні в specs_list — типово результат AttributeMapper.
        # БАГ (історія): без цієї перевірки сира характеристика постачальника
        # у форматі "Вага, кг" (одиниця "запечена" в назву, value="0.495",
        # unit="") матчилась тим самим блоком нижче через base_name == 'вага'
        # і мутувалась IN-PLACE в '495' / unit='г' — тобто СИРЕ значення від
        # постачальника псувалось і потрапляло у CSV замість "0.495" як є,
        # хоча поруч AttributeMapper вже коректно створював окрему
        # характеристику "Вага" = '495' / 'г'. В результаті в CSV
        # дублювались два записи з неправильним сирим.
        # Якщо канонічна "Вага" вже існує окремим записом — сира
        # характеристика з комою в назві більше НЕ конвертується і
        # лишається raw pass-through (як прийшла від постачальника).
        # Якщо канонічної "Вага" немає (постачальник без AttributeMapper) —
        # fallback-конвертація через embedded_unit працює як раніше.
        canonical_weight_present = {
            spec.get('name', '').strip().lower()
            for spec in specs_list
            if spec.get('name', '').strip().lower() in weight_names
        }

        for spec in specs_list:
            spec_name = spec.get('name', '').lower().strip()

            # Деякі постачальники "запікають" одиницю прямо в назву через
            # кому ("Вага, кг") замість окремого поля unit. Відокремлюємо
            # базову назву від можливого суфікса одиниці перед звіркою.
            base_name, _, name_suffix = spec_name.partition(',')
            base_name = base_name.strip()
            name_suffix = name_suffix.strip()
            embedded_unit = name_suffix if name_suffix in ('г', 'кг') else ''

            # «Вага брутто»/«Вес брутто»/«gross weight» — НЕ підлягає SMART-
            # конвертації в required_unit категорії (на відміну від «Вага»/
            # «Вага нетто» нижче). Rozetka (rozetka_dimensions_service.py)
            # завжди хоче кг для «Вага в упаковці», а джерело може дати як
            # кг, так і г — тому зберігаємо СИРЕ число без арифметики і
            # переносимо одиницю в саму назву характеристики ("Вага брутто
            # (кг)" / "Вага брутто (г)"), бо unit="" не завжди доживає до
            # фінального фіда (Prom іноді губить атрибут при експорті).
            if base_name in gross_weight_names:
                self._preserve_gross_weight(spec, spider)
                continue

            if base_name not in weight_names:
                continue

            # Сира характеристика постачальника ("Вага, кг") + поруч вже є
            # канонічна "Вага" (created by AttributeMapper) → не чіпаємо
            # сиру, вона має лишитись точним відображенням значення
            # постачальника (raw pass-through), а не дублювати конвертацію.
            if embedded_unit and base_name in canonical_weight_present:
                continue

            raw_value = spec.get('value', '').strip()
            raw_unit = spec.get('unit', '').strip().lower()

            # Визначаємо число та вихідну одиницю. Пріоритет — суфікс
            # у самому value: це найбільш конкретний і "свіжий" сигнал.
            # Поле unit (або одиниця, запечена в назву) — лише fallback,
            # бо траплялось, що unit виставлений мапінгом/категорією і
            # суперечить тому, що насправді написано у value (напр.
            # value="6.84 кг", unit="г" від віатек-ділера — довіряємо суфіксу у value).
            if raw_value.endswith(' кг'):
                source_unit = 'кг'
                numeric_part = raw_value[:-3].strip()
            elif raw_value.endswith(' г'):
                source_unit = 'г'
                numeric_part = raw_value[:-2].strip()
            elif raw_unit in ('г', 'кг'):
                source_unit = raw_unit
                numeric_part = raw_value
            elif embedded_unit:
                source_unit = embedded_unit
                numeric_part = raw_value
            else:
                continue  # формат незрозумілий — не чіпаємо

            try:
                number = float(numeric_part.replace(',', '.'))
            except ValueError:
                spider.logger.warning(
                    f"⚠️ Помилка конвертації spec ваги: value={raw_value!r} unit={raw_unit!r}"
                )
                continue

            grams = number if source_unit == 'г' else number * 1000

            if required_unit == 'г':
                spec['value'] = ValidationService.sanitize_prom_numeric(str(grams))
                spec['unit'] = 'г'
            else:
                spec['value'] = ValidationService.sanitize_prom_numeric(str(grams / 1000))
                spec['unit'] = 'кг'

            spider.logger.debug(
                f"⚖️ Spec вага (cat={category_id}, unit={required_unit}): "
                f"{raw_value!r} unit={raw_unit!r} → {spec['value']} {spec['unit']}"
            )

        return specs_list

    @staticmethod
    def process_specs_load_capacity(specs_list: list, spider) -> list:
        """
        Постобробка навантаження в характеристиках.
        
        ТІЛЬКИ для портальних характеристик (НЕ для "навантаження" від постачальника):
        - "Маx нагрузка на кронштейн": г → кг (БЕЗ /м)
        - "Максимально допустиме навантаження": г → кг/м
        """
        if not specs_list:
            return specs_list
        
        # Тільки портальні характеристики
        bracket_load_names = [
            'маx нагрузка на кронштейн',
            'max нагрузка на кронштейн',
        ]
        
        permitted_load_names = [
            'максимально допустиме навантаження',
            'максимальная нагрузка',
            'максимальне навантаження',
            'max load capacity',
            'load capacity',
        ]
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower().strip()
            original_value = spec.get('value', '').strip()
            
            # 1. Маx нагрузка на кронштейн: г → кг (БЕЗ /м)
            if spec_name in bracket_load_names:
                if original_value.endswith(' г'):
                    try:
                        grams = float(original_value.replace(' г', '').replace(',', '.'))
                        kg = grams / 1000
                        spec['value'] = ValidationService.sanitize_prom_numeric(str(kg))
                        spec['unit'] = 'кг'
                        spider.logger.debug(
                            f"🔧 Навантаження (кронштейн): {spec['name']} = '{original_value}' → '{spec['value']} кг'"
                        )
                    except ValueError:
                        pass
                
                elif original_value.endswith(' кг'):
                    try:
                        kg = float(original_value.replace(' кг', '').replace(',', '.'))
                        spec['value'] = ValidationService.sanitize_prom_numeric(str(kg))
                        spec['unit'] = 'кг'
                        spider.logger.debug(
                            f"🔧 Навантаження (кронштейн): {spec['name']} = '{original_value}' → '{spec['value']} кг'"
                        )
                    except ValueError:
                        pass
            
            # 2. Максимально допустиме навантаження: г → кг/м
            elif spec_name in permitted_load_names:
                if original_value.endswith(' г'):
                    try:
                        grams = float(original_value.replace(' г', '').replace(',', '.'))
                        kg = grams / 1000
                        spec['value'] = ValidationService.sanitize_prom_numeric(str(kg))
                        spec['unit'] = 'кг/м'
                        spider.logger.debug(
                            f"🔧 Навантаження (допустиме): {spec['name']} = '{original_value}' → '{spec['value']} кг/м'"
                        )
                    except ValueError:
                        pass
                
                elif original_value.endswith(' кг'):
                    try:
                        kg = float(original_value.replace(' кг', '').replace(',', '.'))
                        spec['value'] = ValidationService.sanitize_prom_numeric(str(kg))
                        spec['unit'] = 'кг/м'
                        spider.logger.debug(
                            f"🔧 Навантаження (допустиме): {spec['name']} = '{original_value}' → '{spec['value']} кг/м'"
                        )
                    except ValueError:
                        pass
        
        return specs_list

    @staticmethod
    def process_specs_hdd_capacity(specs_list: list, spider) -> list:
        """
        Постобробка ємності HDD в характеристиках.
        
        Конвертує:
        - "4 SATA 8 Тб" → "32768" (ГБ)
        - "2 Тб" → "2048" (ГБ)
        """
        if not specs_list:
            return specs_list
        
        hdd_names = [
            'суммарная емкость hdd',
            'total hdd capacity',
            'загальна ємність hdd'
        ]
        
        disk_names = [
            'об\'єм накопичувача',
            'disk capacity',
            'ємність диска'
        ]
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower().strip()
            original_value = spec.get('value', '')
            
            # Сумарна ємність HDD: "4 SATA 8 Тб" → 32768 ГБ
            if spec_name in hdd_names:
                match = re.search(r'(\d+)\s*SATA\s*(\d+)\s*[Тт][БбBb]', original_value, re.IGNORECASE)
                if match:
                    try:
                        num_sata = int(match.group(1))
                        max_tb = int(match.group(2))
                        total_gb = num_sata * max_tb * 1024
                        spec['value'] = str(total_gb)
                        spec['unit'] = 'ГБ'
                        spider.logger.debug(
                            f"💾 HDD: {spec['name']} = '{original_value}' → '{total_gb} ГБ'"
                        )
                    except ValueError:
                        pass
            
            # Об'єм накопичувача: "2 Тб" → 2048 ГБ
            elif spec_name in disk_names:
                match = re.search(r'(\d+)\s*[Тт][БбBb]', original_value, re.IGNORECASE)
                if match:
                    try:
                        tb_value = int(match.group(1))
                        gb_value = tb_value * 1024
                        spec['value'] = str(gb_value)
                        spec['unit'] = 'ГБ'
                        spider.logger.debug(
                            f"💾 Диск: {spec['name']} = '{original_value}' → '{gb_value} ГБ'"
                        )
                    except ValueError:
                        pass
        
        return specs_list

    @staticmethod
    def process_specs_battery_capacity(specs_list: list, spider) -> list:
        """
        Постобробка ємності батареї в характеристиках.
        
        Конвертує:
        - "2.5 А·г" → "2500" (мА·г)
        """
        if not specs_list:
            return specs_list
        
        battery_names = [
            'ємність акумулятору',
            'battery capacity',
            'емкость аккумулятора'
        ]
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower().strip()
            
            if spec_name in battery_names:
                original_value = spec.get('value', '')
                # Шукаємо число з А·г / Аг / А-г
                match = re.search(r'([\d\.]+)\s*[АA](?:•|·|г|-)?[гч]?', original_value, re.IGNORECASE)
                if match:
                    try:
                        ah_value = float(match.group(1).replace(',', '.'))
                        mah_value = int(ah_value * 1000)
                        spec['value'] = str(mah_value)
                        spec['unit'] = 'мА·г'
                        spider.logger.debug(
                            f"🔋 Батарея: {spec['name']} = '{original_value}' → '{mah_value} мА·г'"
                        )
                    except ValueError:
                        pass
        
        return specs_list

    @staticmethod
    def extract_dimensions_from_specs(specs_list: list, spider) -> dict:
        """
        Витягує габарити з характеристик для заповнення колонок PROM.
        
        Повертає:
        {
            "Вага,кг": "2,8",
            "Ширина,см": "15,0",
            "Висота,см": "20,5",
            "Довжина,см": "30,0"
        }
        
        ВАЖЛИВО: Колонка AS (Вага,кг) ЗАВЖДИ в кілограмах,
        а одиниці в характеристиках (DD) можуть бути г або кг.
        Всі числові значення нормалізуються через sanitize_prom_numeric
        (кома як десятковий розділювач, без float-артефактів).
        """
        dimensions = {
            "Вага,кг": "",
            "Ширина,см": "",
            "Висота,см": "",
            "Довжина,см": ""
        }
        
        if not specs_list:
            return dimensions

        weight_keys = [
            'вага', 'вага брутто', 'вага нетто',
            'weight', 'gross weight', 'net weight'
        ]
        width_keys = ['ширина', 'width']
        height_keys = ['висота', 'высота', 'height']
        length_keys = ['довжина', 'длина', 'length', 'глибина', 'глубина', 'depth']
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower().strip()
            spec_value = spec.get('value', '').strip()
            spec_unit = spec.get('unit', '').lower().strip()
            
            if not spec_value:
                continue

            # «Вага брутто (кг)» / «Вага брутто (г)»: одиницю "запечено" в
            # назву FieldProcessor._preserve_gross_weight (spec['unit']
            # там навмисно порожній). Знімаємо суфікс перед звіркою з
            # weight_keys і використовуємо його як одиницю, якщо власне
            # поле unit порожнє — щоб ця характеристика й далі коректно
            # заповнювала базову колонку "Вага,кг".
            base_spec_name = spec_name
            name_unit_suffix = ''
            suffix_match = re.match(r'^(.*?)\s*\((кг|г)\)$', spec_name)
            if suffix_match:
                base_spec_name = suffix_match.group(1).strip()
                name_unit_suffix = suffix_match.group(2)
            effective_unit = spec_unit or name_unit_suffix
            
            # 1. ВАГА: колонка AS (Вага,кг) ЗАВЖДИ в кілограмах
            if base_spec_name in weight_keys:
                # Для цих полів Prom.ua приймає тільки крапку як десятковий розділювач;
                # decimals=2 — max 2 знаки після крапки (роздільник тисяч завжди має 3)
                if effective_unit == 'кг':
                    normalized = ValidationService.sanitize_prom_numeric(spec_value, decimals=2, decimal_sep='.')
                    if normalized:
                        dimensions["Вага,кг"] = normalized
                        spider.logger.debug(f"⚖️ Габарит вага: {spec_value} кг → {normalized}")
                
                # Якщо одиниця грами - конвертуємо г → кг
                elif effective_unit == 'г':
                    try:
                        grams = float(spec_value.replace(',', '.'))
                        kg = grams / 1000
                        normalized = ValidationService.sanitize_prom_numeric(str(kg), decimals=2, decimal_sep='.')
                        if normalized:
                            dimensions["Вага,кг"] = normalized
                            spider.logger.debug(f"⚖️ Габарит вага: {grams}г → {normalized}кг")
                    except ValueError:
                        pass
                
                # Якщо немає одиниць - припускаємо грами і конвертуємо
                else:
                    match_num = re.search(r'([0-9\.]+)', spec_value)
                    if match_num:
                        try:
                            grams = float(match_num.group(1).replace(',', '.'))
                            kg = grams / 1000
                            normalized = ValidationService.sanitize_prom_numeric(str(kg), decimals=2, decimal_sep='.')
                            if normalized:
                                dimensions["Вага,кг"] = normalized
                                spider.logger.debug(f"⚖️ Габарит вага: {grams}г → {normalized}кг")
                        except ValueError:
                            pass
            
            # 2. ШИРИНА: мм → см
            elif spec_name in width_keys:
                if spec_unit == 'мм' or 'мм' in spec_value:
                    match_num = re.search(r'([0-9\.]+)', spec_value)
                    if match_num:
                        try:
                            mm = float(match_num.group(1).replace(',', '.'))
                            cm = mm / 10
                            # Крапка; 1 знак після — ніколи не сплутається з роздільником тисяч
                            dimensions["Ширина,см"] = f"{cm:.1f}"
                            spider.logger.debug(f"📏 Габарит ширина: {mm}мм → {cm}см")
                        except ValueError:
                            pass
                elif spec_unit == 'см':
                    try:
                        cm_val = float(spec_value.replace(',', '.'))
                        dimensions["Ширина,см"] = f"{cm_val:.1f}"
                    except ValueError:
                        pass
            
            # 3. ВИСОТА: мм → см
            elif spec_name in height_keys:
                if spec_unit == 'мм' or 'мм' in spec_value:
                    match_num = re.search(r'([0-9\.]+)', spec_value)
                    if match_num:
                        try:
                            mm = float(match_num.group(1).replace(',', '.'))
                            cm = mm / 10
                            dimensions["Висота,см"] = f"{cm:.1f}"
                            spider.logger.debug(f"📏 Габарит висота: {mm}мм → {cm}см")
                        except ValueError:
                            pass
                elif spec_unit == 'см':
                    try:
                        cm_val = float(spec_value.replace(',', '.'))
                        dimensions["Висота,см"] = f"{cm_val:.1f}"
                    except ValueError:
                        pass
            
            # 4. ДОВЖИНА: мм → см
            elif spec_name in length_keys:
                if spec_unit == 'мм' or 'мм' in spec_value:
                    match_num = re.search(r'([0-9\.]+)', spec_value)
                    if match_num:
                        try:
                            mm = float(match_num.group(1).replace(',', '.'))
                            cm = mm / 10
                            dimensions["Довжина,см"] = f"{cm:.1f}"
                            spider.logger.debug(f"📏 Габарит довжина: {mm}мм → {cm}см")
                        except ValueError:
                            pass
                elif spec_unit == 'см':
                    try:
                        cm_val = float(spec_value.replace(',', '.'))
                        dimensions["Довжина,см"] = f"{cm_val:.1f}"
                    except ValueError:
                        pass
        
        return dimensions

    # ------------------------------------------------------------------ #
    # TEXT NORMALISATION
    # ------------------------------------------------------------------ #

    # Таблиця замін: ы→и, э→е, Ы→И, Э→Е
    _RU_CHARS = str.maketrans('ыэЫЭ', 'иеИЕ')

    @classmethod
    def normalize_cyrillic(cls, value: str) -> str:
        """
        Замінює неприпустимі для Kasta російські символи в українському тексті.

        ы → и  (та Ы → И)
        э → е  (та Э → Е)

        Призначено для полів Назва_позиції_укр / Опис_укр.
        Регістр зберігається завдяки str.maketrans.
        """
        if not value:
            return value
        return value.translate(cls._RU_CHARS)
