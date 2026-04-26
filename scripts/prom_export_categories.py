"""
prom_export_categories.py
-------------------------
Синхронізує категорії з фіду PROM з локальними файлами маркетплейсів.
Експортує ТІЛЬКИ ті категорії, під якими є реальні товари у фіді.
Додає НОВІ категорії (яких ще немає по ID) до:
  - data/markets/markets_coefficients.csv  — коефіцієнти для маркетплейсів
  - data/markets/mappings.xlsx (лист 'Категорія+') — маппінг категорій

Запуск:
    python scripts/prom_export_categories.py
"""

import csv
import re
from pathlib import Path

import requests
from openpyxl import load_workbook

FEED_URL = (
    "https://oniks.org.ua/rozetka_feed.xml?rozetka_hash_tag=33ec12f81c283cc0524764696220b10c&product_ids=&label_ids=&languages=uk%2Cru&group_ids=2221523%2C2222437%2C2222561%2C2234751%2C4320349%2C4321341%2C4325742%2C4325743%2C4328775%2C4331550%2C4339717%2C4551903%2C8950007%2C8950011%2C10015559%2C16703618%2C17012111%2C17012456%2C22818554%2C23295147%2C45720479%2C72575633%2C83889367%2C90718784%2C90906797%2C90997501%2C90997677%2C90997694%2C91056839%2C127351905%2C127351912%2C127351948%2C127351950%2C127351973%2C127628160%2C127628166%2C127628170%2C127628173%2C127628176%2C139094517%2C139094704%2C139094708%2C144038788%2C144038790%2C144038804%2C151114178%2C152084397%2C152084437%2C152084460%2C152084594%2C152084669%2C152084678%2C152084703%2C152086699%2C152088176%2C152088624%2C152090354%2C152090439%2C152090654%2C152090742%2C152090999%2C152091016%2C152091894%2C152092523%2C152092600%2C152092613%2C152092625%2C152104228%2C152104243%2C152133169%2C152133408%2C152133464%2C152133483%2C152135823%2C152195979%2C152196244%2C152197115%2C152197317%2C152197474%2C152206635%2C152207998%2C152208073%2C152208101%2C152208132%2C152208469%2C152208563%2C152208591%2C152208632%2C152481182%2C152481185%2C152481192%2C152481294%2C152483771&nested_group_ids=4321341%2C4325742%2C4325743%2C4328775%2C4331550%2C4339717%2C4551903%2C8950007%2C8950011%2C16703618%2C17012111%2C17012456%2C22818554%2C23295147%2C45720479%2C72575633%2C83889367%2C90718784%2C90906797%2C90997501%2C90997677%2C90997694%2C91056839%2C127351912%2C127351948%2C127351950%2C127351973%2C127628160%2C127628166%2C127628170%2C127628173%2C127628176%2C139094704%2C139094708%2C144038788%2C144038790%2C144038804%2C151114178%2C152084397%2C152084437%2C152084460%2C152084594%2C152084669%2C152084678%2C152084703%2C152086699%2C152088176%2C152088624%2C152090354%2C152090439%2C152090654%2C152090742%2C152090999%2C152091016%2C152091894%2C152092523%2C152092600%2C152092613%2C152092625%2C152104243%2C152133169%2C152133408%2C152133464%2C152133483%2C152135823%2C152195979%2C152196244%2C152197115%2C152197317%2C152197474%2C152206635%2C152207998%2C152208073%2C152208101%2C152208132%2C152208469%2C152208563%2C152208591%2C152208632%2C152481182%2C152481185%2C152481192%2C152481294%2C152483771"
)

MAPPINGS = Path(__file__).parents[1] / "data" / "markets" / "mappings.xlsx"
MAPPINGS_SHEET = "Категорія+"
MAPPINGS_ID_COL = "ІD категорії фіду"
MAPPINGS_NAME_COL = "Категорії фіду"

MARKETS_CSV = Path(__file__).parents[1] / "data" / "markets" / "markets_coefficients.csv"


def fetch_xml(url: str) -> str:
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    raw = response.content
    match = re.search(rb'encoding=["\']([^"\']+)["\']', raw[:200])
    encoding = match.group(1).decode("ascii") if match else (response.encoding or "utf-8")
    print(f"🔍 Кодування: {encoding}")
    return raw.decode(encoding)


def parse_categories(xml: str) -> dict[str, dict]:
    """Повертає {id: {name, parentId}} для всіх категорій у фіді."""
    pattern = r'<category\s+id="(\d+)"(?:\s+parentId="(\d+)")?[^>]*>(.*?)</category>'
    categories: dict[str, dict] = {}
    for cat_id, parent_id, name in re.findall(pattern, xml):
        categories[cat_id] = {
            "name": name.strip(),
            "parentId": parent_id or None,
        }
    return categories


def parse_used_category_ids(xml: str) -> set[str]:
    """Повертає set id категорій, які реально використовуються в офферах."""
    return set(re.findall(r"<categoryId>(\d+)</categoryId>", xml))


def build_display_name(cat_id: str, categories: dict[str, dict]) -> str:
    """
    Будує повну назву категорії з батьківською:
    'Батьківська > Дочірня'
    Якщо батька немає — просто назва.
    """
    cat = categories.get(cat_id)
    if not cat:
        return cat_id

    name = cat["name"]
    parent_id = cat["parentId"]

    if parent_id and parent_id in categories:
        parent_name = categories[parent_id]["name"]
        return f"{parent_name} > {name}"

    return name


def main() -> None:
    print("⬇️  Завантаження фіду...")
    xml = fetch_xml(FEED_URL)
    print(f"📄 Отримано {len(xml):,} символів")

    all_categories = parse_categories(xml)
    used_ids = parse_used_category_ids(xml)

    if not all_categories:
        print("⚠️  Категорії не знайдено — перевір URL фіду")
        return

    if not used_ids:
        print("⚠️  Товари не знайдено — перевір структуру фіду")
        return

    # Залишаємо тільки категорії, під якими є товари
    active_categories = {
        cat_id: cat
        for cat_id, cat in all_categories.items()
        if cat_id in used_ids
    }

    skipped = len(all_categories) - len(active_categories)
    print(f"📦 Всього категорій: {len(all_categories)}, з товарами: {len(active_categories)}, пропущено порожніх: {skipped}")

    update_mappings_excel(active_categories, all_categories)
    update_markets_csv(active_categories, all_categories)


def update_mappings_excel(active_categories: dict[str, dict], all_categories: dict[str, dict]) -> None:
    """Дописує в mappings.xlsx лист 'Категорія+' тільки НОВІ категорії (яких ще немає по ID)."""
    if not MAPPINGS.exists():
        print(f"⚠️  mappings.xlsx не знайдено: {MAPPINGS}")
        return

    wb = load_workbook(MAPPINGS)
    if MAPPINGS_SHEET not in wb.sheetnames:
        print(f"⚠️  Лист '{MAPPINGS_SHEET}' не знайдено в {MAPPINGS}")
        return

    ws = wb[MAPPINGS_SHEET]

    # Знаходимо індекси потрібних колонок по заголовку
    header = [cell.value for cell in ws[1]]
    try:
        id_col_idx = header.index(MAPPINGS_ID_COL) + 1   # 1-based
        name_col_idx = header.index(MAPPINGS_NAME_COL) + 1
    except ValueError as e:
        print(f"⚠️  Колонку не знайдено: {e}")
        return

    # Збираємо вже існуючі ID (рядки 2+)
    existing_ids: set[str] = set()
    for row in ws.iter_rows(min_row=2, values_only=True):
        val = row[id_col_idx - 1]
        if val is not None:
            existing_ids.add(str(val))

    # Визначаємо нові категорії
    new_categories = {
        cat_id: cat
        for cat_id, cat in active_categories.items()
        if str(cat_id) not in existing_ids
    }

    if not new_categories:
        print("✅ Нових категорій немає — mappings.xlsx не змінено")
        return

    # Дописуємо нові рядки вниз
    for cat_id, cat in sorted(new_categories.items(), key=lambda x: int(x[0])):
        display_name = build_display_name(cat_id, all_categories)
        new_row = [None] * len(header)
        new_row[id_col_idx - 1] = int(cat_id)
        new_row[name_col_idx - 1] = display_name
        ws.append(new_row)

    wb.save(MAPPINGS)
    print(f"✅ Додано {len(new_categories)} нових категорій → {MAPPINGS} (лист '{MAPPINGS_SHEET}')")
    for cat_id in sorted(new_categories, key=lambda x: int(x)):
        print(f"   + [{cat_id}] {build_display_name(cat_id, all_categories)}")


def update_markets_csv(active_categories: dict[str, dict], all_categories: dict[str, dict]) -> None:
    """Дописує в markets_coefficients.csv тільки НОВІ категорії з дефолтними коефіцієнтами."""
    if not MARKETS_CSV.exists():
        print(f"⚠️  markets_coefficients.csv не знайдено: {MARKETS_CSV}")
        return

    # Зчитуємо існуючі рядки та визначаємо існуючі ID
    with MARKETS_CSV.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        fieldnames = reader.fieldnames or []
        rows = list(reader)

    existing_ids = {row["category_id"] for row in rows}

    # Беремо дефолтні коефіцієнти з останнього рядка файлу
    if rows:
        last = rows[-1]
        default_coefs = {
            "coef_kasta":     last.get("coef_kasta", "1.02"),
            "coef_epicenter": last.get("coef_epicenter", "1.22"),
            "coef_rozetka":   last.get("coef_rozetka", "1.32"),
        }
    else:
        default_coefs = {"coef_kasta": "1.02", "coef_epicenter": "1.22", "coef_rozetka": "1.32"}

    new_categories = {
        cat_id: cat
        for cat_id, cat in active_categories.items()
        if cat_id not in existing_ids
    }

    if not new_categories:
        print("✅ Нових категорій немає — markets_coefficients.csv не змінено")
        return

    with MARKETS_CSV.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        for cat_id, _ in sorted(new_categories.items(), key=lambda x: int(x[0])):
            writer.writerow({
                "category_id":   cat_id,
                "category_name": build_display_name(cat_id, all_categories),
                **default_coefs,
            })

    print(f"✅ Додано {len(new_categories)} нових категорій → {MARKETS_CSV}")
    for cat_id in sorted(new_categories, key=lambda x: int(x)):
        print(f"   + [{cat_id}] {build_display_name(cat_id, all_categories)}  {default_coefs['coef_kasta']};{default_coefs['coef_epicenter']};{default_coefs['coef_rozetka']}")


if __name__ == "__main__":
    main()
