#!/usr/bin/env python3
# -*- coding: utf-8 -*-
r"""
Універсальний скрипт порівняння та оновлення товарів для всіх постачальників.
Підтримує типи: dealer, retail

Порівняння товарів по Ідентифікатор_товару (артикул постачальника)

ОНОВЛЕНО:
- Додано відстеження зміни ціни (колонка PROM: "Ціна"):
  при зміні ціни товар потрапляє у файл імпорту
- Статистика: одна строка "Змінилася ціна" (сума по всім кейсам)
- Універсальна логіка для всіх постачальників:
  OLD файл: C:\FullStack\PriceFeedPipeline\data\{supplier}\{supplier}_old.csv
  NEW файл: C:\FullStack\PriceFeedPipeline\data\output\{supplier}_new.csv
- Після генерації import_products.csv видаляється старий {supplier}_old.csv
  і {supplier}_new.csv перейменовується в {supplier}_old.csv
"""

import csv
import os
import re
import sys
from typing import Dict, List, Set, Tuple


# Єдиний реєстр постачальників.
# При додаванні нового — тільки сюди.
# spider: ім'я паука для ultra_clean_run.py
# type:   тип для логіки pipeline
SUPPLIER_CONFIG: dict[str, dict[str, str]] = {
    "viatec":   {"spider": "viatec_dealer",  "type": "dealer"},
    "secur":    {"spider": "secur_feed_full", "type": "retail"},
    # Нові постачальники — додавати тут:
    # "neolight": {"spider": "neolight_retail", "type": "retail"},
    # "lun":      {"spider": "lun_retail",      "type": "retail"},
}

# Зворотна сумісність зі старим кодом (якщо десь використовується)
SUPPLIERS = list(SUPPLIER_CONFIG.keys())
TYPES = ['dealer', 'retail']


def detect_encoding(file_path: str) -> str:
    """Автоматично визначає кодування файлу."""
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)

        # BOM UTF-8
        if raw_data.startswith(b'\xef\xbb\xbf'):
            return 'utf-8-sig'

        encodings_to_try = ['utf-8', 'utf-8-sig', 'windows-1251', 'cp1251', 'latin-1']
        for encoding in encodings_to_try:
            try:
                raw_data.decode(encoding)
                return encoding
            except (UnicodeDecodeError, LookupError):
                continue

    except Exception as e:
        print(f"⚠️  Помилка визначення кодування: {e}")

    return 'utf-8-sig'


def read_csv_as_rows(file_path: str) -> Tuple[List[List[str]], List[str]]:
    """Читає CSV як список рядків з автоматичним визначенням кодування."""
    rows: List[List[str]] = []
    headers: List[str] = []

    try:
        encoding = detect_encoding(file_path)
        print(f"🔍 Кодування: {encoding}")

        with open(file_path, 'r', encoding=encoding, errors='replace') as f:
            reader = csv.reader(f, delimiter=';')
            headers = next(reader)
            print(f"📋 Заголовки: {headers[:3]}...")

            for row in reader:
                rows.append(row)

        print(f"✅ Прочитано {len(rows)} товарів з {os.path.basename(file_path)}")
        return rows, headers

    except FileNotFoundError:
        print(f"❌ Файл не знайдено: {file_path}")
        return [], []
    except Exception as e:
        print(f"❌ Помилка читання: {e}")
        import traceback
        traceback.print_exc()
        return [], []


def get_field_index(headers: List[str], field_name: str) -> int:
    """Повертає індекс поля або -1."""
    try:
        return headers.index(field_name)
    except ValueError:
        return -1


def get_characteristics_start_index(headers: List[str]) -> int:
    """Індекс початку характеристик (після 'Де_знаходиться_товар')."""
    idx = get_field_index(headers, "Де_знаходиться_товар")
    return idx + 1 if idx != -1 else len(headers)


def safe_get(row: List[str], idx: int) -> str:
    return row[idx] if 0 <= idx < len(row) else ""


def ensure_row_len(row: List[str], target_len: int) -> List[str]:
    if len(row) < target_len:
        row = row + [""] * (target_len - len(row))
    return row


_price_clean_re = re.compile(r"[^\d,.\-]+")


def normalize_price(raw: str) -> str:
    """
    Нормалізує ціну для порівняння:
    - прибирає пробіли/валюту/текст
    - коми → крапки
    - приводить до канонічного вигляду (без зайвих нулів)
    """
    s = (raw or "").strip()
    if not s:
        return ""

    s = _price_clean_re.sub("", s)
    s = s.replace(",", ".")

    # якщо кілька крапок (інколи тисячі), залишаємо останню як десяткову
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]

    try:
        val = float(s)
        return f"{val:.6f}".rstrip("0").rstrip(".")
    except Exception:
        return s


def merge_rows(
    old_row: List[str],
    new_row: List[str],
    old_headers: List[str],
    availability_idx: int,
    quantity_idx: int,
    price_idx: int,
    chars_start_idx: int
) -> List[str]:
    """
    Об'єднує рядки:
    - базові поля зі старого
    - Наявність/Кількість/Ціна + характеристики з нового
    """
    merged = ensure_row_len(old_row.copy(), len(old_headers))

    if availability_idx != -1:
        merged[availability_idx] = safe_get(new_row, availability_idx)

    if quantity_idx != -1:
        merged[quantity_idx] = safe_get(new_row, quantity_idx)

    if price_idx != -1:
        merged[price_idx] = safe_get(new_row, price_idx)

    # характеристики — повністю з нового
    merged = merged[:chars_start_idx]
    if chars_start_idx < len(new_row):
        merged.extend(new_row[chars_start_idx:])

    return ensure_row_len(merged, len(old_headers))


def build_products_dict(
    rows: List[List[str]],
    identifier_idx: int,
    name_idx: int,
    code_idx: int
) -> Tuple[Dict[str, List[str]], List[str], List[Tuple[str, str]]]:
    """
    Повертає:
    - dict: {identifier: row}
    - список описів рядків без identifier
    - список дублікатів (name, identifier)
    """
    products: Dict[str, List[str]] = {}
    no_identifier: List[str] = []
    duplicates: List[Tuple[str, str]] = []

    for row in rows:
        identifier = safe_get(row, identifier_idx).strip() if identifier_idx != -1 else ""
        if not identifier:
            product_name = safe_get(row, name_idx).strip() if name_idx != -1 else "N/A"
            code = safe_get(row, code_idx).strip() if code_idx != -1 else "N/A"
            no_identifier.append(f"{product_name[:40]}... | Код: {code}")
            continue

        if identifier in products:
            product_name = safe_get(row, name_idx).strip() if name_idx != -1 else "N/A"
            duplicates.append((product_name, identifier))
            continue

        products[identifier] = row

    return products, no_identifier, duplicates


def finalize_supplier_files(supplier: str, base_path: str) -> None:
    """
    Фінальні операції для постачальника:
    1. Видалити data/{supplier}/{supplier}_old.csv
    2. Перейменувати data/output/{supplier}_new.csv -> data/{supplier}/{supplier}_old.csv
    """
    old_file = os.path.join(base_path, "data", supplier, f"{supplier}_old.csv")
    new_file = os.path.join(base_path, "data", "output", f"{supplier}_new.csv")
    target_file = os.path.join(base_path, "data", supplier, f"{supplier}_old.csv")

    print(f"\n{'='*60}")
    print(f"🔄 ФІНАЛЬНІ ОПЕРАЦІЇ ДЛЯ {supplier.upper()}")
    print(f"{'='*60}")

    # Видаляємо старий файл, якщо він існує
    if os.path.exists(old_file):
        try:
            os.remove(old_file)
            print(f"✅ Видалено старий файл: {supplier}_old.csv")
        except Exception as e:
            print(f"⚠️  Помилка видалення {old_file}: {e}")
    else:
        print(f"ℹ️  Старий файл {supplier}_old.csv не знайдено (можливо, це перший запуск)")

    # Перейменовуємо новий файл
    if os.path.exists(new_file):
        try:
            os.makedirs(os.path.dirname(target_file), exist_ok=True)
            os.rename(new_file, target_file)
            print(f"✅ Перейменовано: {supplier}_new.csv -> {supplier}_old.csv")
        except Exception as e:
            print(f"❌ Помилка перейменування {new_file}: {e}")
    else:
        print(f"⚠️  Файл {new_file} не знайдено для перейменування")

    print(f"{'='*60}")


def process_supplier(supplier: str, product_type: str) -> None:
    print(f"\n{'='*60}")
    print(f"🔄 {supplier.upper()} - {product_type.upper()}")
    print(f"{'='*60}")

    # Підтримує локальний запуск (дефолт) і GitHub Actions (через env PROJECT_ROOT)
    base_path = os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline")

    # GitHub Actions передає {SUPPLIER}_OK=false якщо паук завершився з помилкою.
    # Локально змінна не встановлена → за замовчуванням 'true' → обробка йде.
    supplier_ok = os.environ.get(f"{supplier.upper()}_OK", "true").strip().lower()
    if supplier_ok == "false":
        print(f"⏭️  {supplier.upper()} ПРОПУЩЕНО — паук завершився з помилкою.")
        print(f"   Старі дані збережено без змін до наступного успішного циклу.")
        return

    # Універсальна логіка для всіх постачальників
    old_file = os.path.join(base_path, "data", supplier, f"{supplier}_old.csv")
    new_file = os.path.join(base_path, "data", "output", f"{supplier}_new.csv")
    import_file = os.path.join(base_path, "data", supplier, "import_products.csv")

    if not os.path.exists(old_file):
        print(f"❌ OLD файл не знайдено: {old_file}")
        print(f"ℹ️  Очікуваний шлях: data/{supplier}/{supplier}_old.csv")
        return

    if not os.path.exists(new_file):
        print(f"❌ NEW файл не знайдено: {new_file}")
        print(f"ℹ️  Очікуваний шлях: data/output/{supplier}_new.csv")
        return

    print(f"\n📂 Читаємо {os.path.basename(old_file)}...")
    old_rows, old_headers = read_csv_as_rows(old_file)

    print(f"\n📂 Читаємо {os.path.basename(new_file)}...")
    new_rows, new_headers = read_csv_as_rows(new_file)

    if not old_rows or not new_rows:
        print("❌ Не вдалося прочитати файли")
        return

    # Захист від часткового парсингу: якщо новий файл має < 80% рядків від старого
    # — це ознака збою спайдера. Не обробляємо, щоб не зняти товари з продажу.
    if len(old_rows) > 0:
        ratio = len(new_rows) / len(old_rows)
        if ratio < 0.80:
            print(f"\n🛑 ЗАХИСТ: новий файл має лише {len(new_rows)} рядків "
                  f"vs {len(old_rows)} старих ({ratio:.0%}).")
            print(f"   Поріг: 80%. Пропускаємо обробку, щоб не зняти товари.")
            print(f"   Можлива причина: спайдер впав на середині, дані неповні.")
            return

    name_idx = get_field_index(old_headers, "Назва_позиції")
    code_idx = get_field_index(old_headers, "Код_товару")
    availability_idx = get_field_index(old_headers, "Наявність")
    quantity_idx = get_field_index(old_headers, "Кількість")
    identifier_idx = get_field_index(old_headers, "Ідентифікатор_товару")
    price_idx = get_field_index(old_headers, "Ціна")
    chars_start_idx = get_characteristics_start_index(old_headers)

    if name_idx == -1:
        print("❌ Не знайдено колонку 'Назва_позиції'")
        return
    if identifier_idx == -1:
        print("❌ Не знайдено колонку 'Ідентифікатор_товару'")
        return
    if availability_idx == -1 or quantity_idx == -1:
        print("❌ Не знайдено колонку 'Наявність' або 'Кількість'")
        return
    if price_idx == -1:
        print("⚠️  Не знайдено колонку 'Ціна' (перевірка ціни буде пропущена).")

    old_products_dict, old_no_identifier, old_duplicates = build_products_dict(
        old_rows, identifier_idx, name_idx, code_idx
    )
    new_products_dict, new_no_identifier, new_duplicates = build_products_dict(
        new_rows, identifier_idx, name_idx, code_idx
    )

    print(f"\n📊 Старих товарів (з ідентифікатором): {len(old_products_dict)}")
    print(f"📊 Нових товарів (з ідентифікатором):  {len(new_products_dict)}")

    if old_no_identifier or old_duplicates or new_no_identifier or new_duplicates:
        print(f"\n{'-'*60}")
        print("⚠️  ФІЛЬТРАЦІЯ ТОВАРІВ:")
        print(f"{'-'*60}")

        if old_no_identifier:
            print(f"\n🚫 Без ідентифікатора в {os.path.basename(old_file)}: {len(old_no_identifier)}")
            for item in old_no_identifier[:5]:
                print(f"   - {item}")
            if len(old_no_identifier) > 5:
                print(f"   ... та ще {len(old_no_identifier) - 5}")

        if old_duplicates:
            print(f"\n🔁 Дублікати ідентифікаторів в {os.path.basename(old_file)}: {len(old_duplicates)}")
            for name, identifier in old_duplicates[:5]:
                print(f"   - '{name}' | ID: '{identifier}'")
            if len(old_duplicates) > 5:
                print(f"   ... та ще {len(old_duplicates) - 5}")

        if new_no_identifier:
            print(f"\n🚫 Без ідентифікатора в {os.path.basename(new_file)}: {len(new_no_identifier)}")
            for item in new_no_identifier[:5]:
                print(f"   - {item}")
            if len(new_no_identifier) > 5:
                print(f"   ... та ще {len(new_no_identifier) - 5}")

        if new_duplicates:
            print(f"\n🔁 Дублікати ідентифікаторів в {os.path.basename(new_file)}: {len(new_duplicates)}")
            for name, identifier in new_duplicates[:5]:
                print(f"   - '{name}' | ID: '{identifier}'")
            if len(new_duplicates) > 5:
                print(f"   ... та ще {len(new_duplicates) - 5}")

        print(f"{'-'*60}")

    import_rows: List[List[str]] = []
    processed_identifiers: Set[str] = set()

    stats = {
        "unchanged": 0,
        "qty_changed": 0,
        "availability_changed": 0,
        "both_changed": 0,
        "price_changed": 0,
        "chars_changed": 0,
        "not_in_new": 0,
        "already_unavailable": 0,
        "new_products": 0,
    }

    for identifier, old_row in old_products_dict.items():
        processed_identifiers.add(identifier)

        if identifier in new_products_dict:
            new_row = new_products_dict[identifier]

            old_availability = safe_get(old_row, availability_idx).strip()
            new_availability = safe_get(new_row, availability_idx).strip()

            old_quantity = safe_get(old_row, quantity_idx).strip()
            new_quantity = safe_get(new_row, quantity_idx).strip()

            availability_changed = old_availability != new_availability
            quantity_changed = old_quantity != new_quantity

            price_changed = False
            if price_idx != -1:
                old_price = normalize_price(safe_get(old_row, price_idx))
                new_price = normalize_price(safe_get(new_row, price_idx))
                price_changed = (old_price != new_price)

            if price_changed:
                stats["price_changed"] += 1

            # Перевіряємо зміни в характеристиках (після Де_знаходиться_товар)
            chars_changed = False
            old_chars = old_row[chars_start_idx:] if chars_start_idx < len(old_row) else []
            new_chars = new_row[chars_start_idx:] if chars_start_idx < len(new_row) else []
            max_len = max(len(old_chars), len(new_chars))
            old_chars_padded = old_chars + [""] * (max_len - len(old_chars))
            new_chars_padded = new_chars + [""] * (max_len - len(new_chars))
            if old_chars_padded != new_chars_padded:
                chars_changed = True
                stats["chars_changed"] += 1

            if not availability_changed and not quantity_changed and not price_changed and not chars_changed:
                stats["unchanged"] += 1
                continue

            updated_row = merge_rows(
                old_row=old_row,
                new_row=new_row,
                old_headers=old_headers,
                availability_idx=availability_idx,
                quantity_idx=quantity_idx,
                price_idx=price_idx,
                chars_start_idx=chars_start_idx,
            )

            if availability_changed and quantity_changed:
                stats["both_changed"] += 1
            elif quantity_changed:
                stats["qty_changed"] += 1
            elif availability_changed:
                stats["availability_changed"] += 1

            import_rows.append(updated_row)

        else:
            old_availability = safe_get(old_row, availability_idx).strip()
            old_quantity = safe_get(old_row, quantity_idx).strip()

            if old_availability == "-" and old_quantity == "0":
                stats["already_unavailable"] += 1
                continue

            updated_row = ensure_row_len(old_row.copy(), len(old_headers))
            updated_row[availability_idx] = "-"
            updated_row[quantity_idx] = "0"

            import_rows.append(updated_row)
            stats["not_in_new"] += 1

    new_product_identifiers = set(new_products_dict.keys()) - processed_identifiers
    if new_product_identifiers:
        for new_identifier in sorted(new_product_identifiers):
            new_row = ensure_row_len(new_products_dict[new_identifier].copy(), len(old_headers))
            import_rows.append(new_row)
            stats["new_products"] += 1

    try:
        os.makedirs(os.path.dirname(import_file), exist_ok=True)
        with open(import_file, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(old_headers)
            for row in import_rows:
                writer.writerow(row[:len(old_headers)])
        print(f"\n✅ Файл створено: {import_file}")
    except Exception as e:
        print(f"❌ Помилка запису: {e}")
        return

    print(f"\n{'='*60}")
    print("📈 СТАТИСТИКА:")
    print(f"{'='*60}")
    print(f"  Без змін:                {stats['unchanged']}")
    print(f"  Змінилася кількість:     {stats['qty_changed']}")
    print(f"  Змінилася наявність:     {stats['availability_changed']}")
    print(f"  Змінилося обидва:        {stats['both_changed']}")
    print(f"  Змінилася ціна:          {stats['price_changed']}")
    print(f"  Змінились характеристики: {stats['chars_changed']}")
    print(f"  Відсутні в новому:       {stats['not_in_new']}")
    print(f"  Вже були відсутні:       {stats['already_unavailable']}")
    print(f"  Нові товари:             {stats['new_products']}")
    print(f"{'-'*60}")
    print(f"  ВСЬОГО для імпорту:      {len(import_rows)}")
    print(f"{'='*60}")

    # Виконуємо фінальні операції для всіх постачальників
    finalize_supplier_files(supplier, base_path)


def main() -> None:
    print("=" * 60)
    print("🚀 УНІВЕРСАЛЬНИЙ СКРИПТ ОНОВЛЕННЯ ТОВАРІВ")
    print("   (Порівняння по Ідентифікатор_товару)")
    print("=" * 60)

    if len(sys.argv) == 1:
        print("\n📦 Обробка всіх постачальників з SUPPLIER_CONFIG...")
        for supplier, config in SUPPLIER_CONFIG.items():
            try:
                process_supplier(supplier, config["type"])
            except Exception as e:
                print(f"❌ Помилка {supplier} {config['type']}: {e}")
        print("\n✅ ВСІ ПОСТАЧАЛЬНИКИ ОБРОБЛЕНО")
        return

    if len(sys.argv) < 3:
        print("\n❌ Використання: python update_products.py <supplier> <type>")
        print(f"\nПостачальники: {', '.join(SUPPLIERS)}")
        print(f"Типи: {', '.join(TYPES)}")
        sys.exit(1)

    supplier = sys.argv[1].lower()
    product_type = sys.argv[2].lower()

    if supplier not in SUPPLIER_CONFIG:
        print(f"❌ Невідомий постачальник: {supplier}")
        print(f"Доступні: {', '.join(SUPPLIER_CONFIG.keys())}")
        sys.exit(1)

    if product_type not in TYPES:
        print(f"❌ Невідомий тип: {product_type}")
        print(f"Доступні: {', '.join(TYPES)}")
        sys.exit(1)

    process_supplier(supplier, product_type)
    print("\n✅ ЗАВЕРШЕНО")


if __name__ == "__main__":
    main()
