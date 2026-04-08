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


class FieldProcessor:
    """Постобробка полів для конвертації одиниць виміру"""
    
    def __init__(self, category_config_path: str | Path = None):
        """
        Ініціалізація з опціональним завантаженням конфігурації категорій.
        
        Args:
            category_config_path: шлях до viatec_category_dealer.csv
        """
        self.category_weight_units = {}  # {category_id: 'г' or 'кг'}
        
        if category_config_path:
            self._load_category_config(category_config_path)
    
    def _load_category_config(self, path: str | Path):
        """Завантажує конфігурацію одиниць виміру для категорій"""
        try:
            with open(path, encoding='utf-8-sig') as f:
                reader = csv.DictReader(f, delimiter=';')
                for row in reader:
                    category_id = row.get('Ідентифікатор_підрозділу', '').strip()
                    weight_unit = row.get('Одиниця_виміру_Характеристики', '').strip()
                    
                    if category_id and weight_unit:
                        self.category_weight_units[category_id] = weight_unit
            
            # Детальне логування
            g_categories = [k for k, v in self.category_weight_units.items() if v == 'г']
            kg_categories = [k for k, v in self.category_weight_units.items() if v == 'кг']
            print(f"✅ Завантажено {len(self.category_weight_units)} категорій з {path.name}")
            print(f"   Категорії з 'г': {g_categories[:10]}..." if len(g_categories) > 10 else f"   Категорії з 'г': {g_categories}")
            print(f"   Категорії з 'кг': {kg_categories[:10]}..." if len(kg_categories) > 10 else f"   Категорії з 'кг': {kg_categories}")
        
        except Exception as e:
            print(f"⚠️ Помилка завантаження category_config: {e}")

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
            Число для PROM (без одиниць)
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
        match_g = re.match(r'([0-9\.]+)\s*г$', value)
        match_kg = re.match(r'([0-9\.]+)\s*кг$', value)
        
        if match_g:
            grams = float(match_g.group(1).replace(',', '.'))
            
            if required_unit == 'г':
                # PROM вимагає грами - залишаємо як є
                result = str(int(grams)) if grams == int(grams) else str(grams).replace('.', ',')
                spider.logger.debug(f"⚖️ Вага (cat={category_id}, unit=г): {value} → {result} г")
                return result
            else:
                # PROM вимагає кг - конвертуємо г → кг
                kg = grams / 1000
                # Форматуємо без зайвих нулів: 2.8 замість 2.800
                result = str(kg).replace('.', ',')
                spider.logger.debug(f"⚖️ Вага (cat={category_id}, unit=кг): {value} → {result} кг")
                return result
        
        elif match_kg:
            kg = float(match_kg.group(1).replace(',', '.'))
            
            if required_unit == 'г':
                # PROM вимагає грами - конвертуємо кг → г
                grams = kg * 1000
                result = str(int(grams)) if grams == int(grams) else str(grams).replace('.', ',')
                spider.logger.debug(f"⚖️ Вага (cat={category_id}, unit=г): {value} → {result} г")
                return result
            else:
                # PROM вимагає кг - залишаємо як є
                result = str(kg).replace('.', ',')
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
                result = f"{cm:.1f}".replace('.', ',')
                spider.logger.debug(f"📏 {field_name}: {value} → {result} см")
                return result
            except ValueError:
                spider.logger.warning(f"⚠️ Помилка конвертації {field_name}: {value}")
                return value
        
        # Розмір в сантиметрах: "15 см" → 15 см
        elif value.endswith(' см'):
            try:
                cm = float(value.replace(' см', '').replace(',', '.'))
                result = f"{cm:.1f}".replace('.', ',')
                spider.logger.debug(f"📏 {field_name}: {value} → {result} см")
                return result
            except ValueError:
                spider.logger.warning(f"⚠️ Помилка конвертації {field_name}: {value}")
                return value
        
        # Якщо одиниць немає - залишаємо як є
        return value

    def process_specs_weight(self, specs_list: list, category_id: str, spider) -> list:
        """
        SMART постобробка ваги в характеристиках.
        
        Використовує конфігурацію категорії для визначення одиниць.
        - Категорія вимагає 'г': "300 г" → "300" + unit="г"
        - Категорія вимагає 'кг': "300 г" → "0,3" + unit="кг"
        """
        if not specs_list:
            return specs_list
        
        # Визначаємо вимоги PROM для цієї категорії
        required_unit = self.category_weight_units.get(category_id, 'г')
        
        weight_names = [
            'вага', 'вага брутто', 'вага нетто',
            'weight', 'gross weight', 'net weight'
        ]
        
        for spec in specs_list:
            spec_name = spec.get('name', '').lower().strip()
            
            if spec_name in weight_names:
                original_value = spec.get('value', '').strip()
                
                # Конвертація г → потрібна одиниця
                if original_value.endswith(' г'):
                    try:
                        grams = float(original_value.replace(' г', '').replace(',', '.'))
                        
                        if required_unit == 'г':
                            # PROM вимагає грами - залишаємо як є
                            spec['value'] = str(int(grams)) if grams == int(grams) else str(grams).replace('.', ',')
                            spec['unit'] = 'г'
                            spider.logger.debug(
                                f"⚖️ Spec вага (cat={category_id}, unit=г): {original_value} → {spec['value']} г"
                            )
                        else:
                            # PROM вимагає кг - конвертуємо
                            kg = grams / 1000
                            spec['value'] = str(kg).replace('.', ',')
                            spec['unit'] = 'кг'
                            spider.logger.debug(
                                f"⚖️ Spec вага (cat={category_id}, unit=кг): {original_value} → {kg} кг"
                            )
                    except ValueError:
                        spider.logger.warning(
                            f"⚠️ Помилка конвертації spec ваги: {original_value}"
                        )
                
                # Якщо вже в кг
                elif original_value.endswith(' кг'):
                    try:
                        kg = float(original_value.replace(' кг', '').replace(',', '.'))
                        
                        if required_unit == 'г':
                            # PROM вимагає грами - конвертуємо
                            grams = kg * 1000
                            spec['value'] = str(int(grams)) if grams == int(grams) else str(grams).replace('.', ',')
                            spec['unit'] = 'г'
                            spider.logger.debug(
                                f"⚖️ Spec вага (cat={category_id}, unit=г): {original_value} → {grams} г"
                            )
                        else:
                            # PROM вимагає кг - залишаємо як є
                            spec['value'] = str(kg).replace('.', ',')
                            spec['unit'] = 'кг'
                            spider.logger.debug(
                                f"⚖️ Spec вага (cat={category_id}, unit=кг): {original_value} → {kg} кг"
                            )
                    except ValueError:
                        pass
        
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
                        spec['value'] = str(kg).replace('.', ',')
                        spec['unit'] = 'кг'
                        spider.logger.debug(
                            f"🔧 Навантаження (кронштейн): {spec['name']} = '{original_value}' → '{kg} кг'"
                        )
                    except ValueError:
                        pass
                
                elif original_value.endswith(' кг'):
                    try:
                        kg = float(original_value.replace(' кг', '').replace(',', '.'))
                        spec['value'] = str(kg).replace('.', ',')
                        spec['unit'] = 'кг'
                        spider.logger.debug(
                            f"🔧 Навантаження (кронштейн): {spec['name']} = '{original_value}' → '{kg} кг'"
                        )
                    except ValueError:
                        pass
            
            # 2. Максимально допустиме навантаження: г → кг/м
            elif spec_name in permitted_load_names:
                if original_value.endswith(' г'):
                    try:
                        grams = float(original_value.replace(' г', '').replace(',', '.'))
                        kg = grams / 1000
                        spec['value'] = str(kg).replace('.', ',')
                        spec['unit'] = 'кг/м'
                        spider.logger.debug(
                            f"🔧 Навантаження (допустиме): {spec['name']} = '{original_value}' → '{kg} кг/м'"
                        )
                    except ValueError:
                        pass
                
                elif original_value.endswith(' кг'):
                    try:
                        kg = float(original_value.replace(' кг', '').replace(',', '.'))
                        spec['value'] = str(kg).replace('.', ',')
                        spec['unit'] = 'кг/м'
                        spider.logger.debug(
                            f"🔧 Навантаження (допустиме): {spec['name']} = '{original_value}' → '{kg} кг/м'"
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
            
            # 1. ВАГА: колонка AS (Вага,кг) ЗАВЖДИ в кілограмах
            if spec_name in weight_keys:
                # Якщо одиниця вже кг - залишаємо як є
                if spec_unit == 'кг':
                    dimensions["Вага,кг"] = spec_value
                    spider.logger.debug(f"⚖️ Габарит вага: {spec_value} кг")
                
                # Якщо одиниця грами - конвертуємо г → кг
                elif spec_unit == 'г':
                    try:
                        grams = float(spec_value.replace(',', '.'))
                        kg = grams / 1000
                        dimensions["Вага,кг"] = str(kg).replace('.', ',')
                        spider.logger.debug(f"⚖️ Габарит вага: {grams}г → {kg}кг")
                    except ValueError:
                        pass
                
                # Якщо немає одиниць - припускаємо грами і конвертуємо
                else:
                    match_num = re.search(r'([0-9\.]+)', spec_value)
                    if match_num:
                        try:
                            grams = float(match_num.group(1).replace(',', '.'))
                            kg = grams / 1000
                            dimensions["Вага,кг"] = str(kg).replace('.', ',')
                            spider.logger.debug(f"⚖️ Габарит вага: {grams}г → {kg}кг")
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
                            dimensions["Ширина,см"] = f"{cm:.1f}".replace('.', ',')
                            spider.logger.debug(f"📏 Габарит ширина: {mm}мм → {cm}см")
                        except ValueError:
                            pass
                elif spec_unit == 'см':
                    dimensions["Ширина,см"] = spec_value
            
            # 3. ВИСОТА: мм → см
            elif spec_name in height_keys:
                if spec_unit == 'мм' or 'мм' in spec_value:
                    match_num = re.search(r'([0-9\.]+)', spec_value)
                    if match_num:
                        try:
                            mm = float(match_num.group(1).replace(',', '.'))
                            cm = mm / 10
                            dimensions["Висота,см"] = f"{cm:.1f}".replace('.', ',')
                            spider.logger.debug(f"📏 Габарит висота: {mm}мм → {cm}см")
                        except ValueError:
                            pass
                elif spec_unit == 'см':
                    dimensions["Висота,см"] = spec_value
            
            # 4. ДОВЖИНА: мм → см
            elif spec_name in length_keys:
                if spec_unit == 'мм' or 'мм' in spec_value:
                    match_num = re.search(r'([0-9\.]+)', spec_value)
                    if match_num:
                        try:
                            mm = float(match_num.group(1).replace(',', '.'))
                            cm = mm / 10
                            dimensions["Довжина,см"] = f"{cm:.1f}".replace('.', ',')
                            spider.logger.debug(f"📏 Габарит довжина: {mm}мм → {cm}см")
                        except ValueError:
                            pass
                elif spec_unit == 'см':
                    dimensions["Довжина,см"] = spec_value
        
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
