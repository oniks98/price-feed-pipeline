Треба зробити рефактор
C:\FullStack\PriceFeedPipeline\scripts\epicenter_export_categories.py

Зараз скрипт не бачить , якщо в C:\FullStack\PriceFeedPipeline\data\markets\mappings.xlsx додались нові категорії.

Допиши нову логіку правильно прямо в скрипті

mappings.xlsx ← дописали 3 категорії
│
▼
Запуск 1: epicenter_export_categories.py
→ «Маппінг» +3 рядки
→ ЛОГ: "Заповни epicenter_category_id → запусти ще раз"
→ СТОП
│
├─ epicenter_map_categories.py (авто)
└─ або вручну C, D, E
│
▼
Запуск 2: epicenter_export_categories.py
→ «Сети атрибутів» +N рядків для нових set_codes
→ ЛОГ: "Заповни prom_param_name (колонка J) → запусти ще раз"
→ СТОП
│
├─ epicenter_map_attributes.py (авто)
└─ або вручну заповнити J
│
▼
Запуск 3: epicenter_export_categories.py
→ «Опції атрибутів» +M рядків тільки для нових
→ Старі дані не чіпає ✅

Корінна проблема — відсутня нормалізація ідентифікаторів. Excel зберігає числа як float (123.0), str(123.0) = "123.0", а нова категорія набрана вручну дає "123" → "123.0" != "123" → скрипт або бачить нові як вже існуючі, або навпаки. Окрім цього: некоректне закриття Workbook в append_new_prom_categories (ранній close() + відсутній close() в happy-path), і потенційно хибний id_col detection.

Нова функція — які set_codes вже є в «Сети атрибутів»
def load_set_codes_in_attr_sets() -> set[str]:
"""
Повертає set_codes, які вже є в листі «Сети атрибутів».
"""
if not OUTPUT_PATH.exists():
return set()
try:
import openpyxl as \_xl
wb = \_xl.load_workbook(OUTPUT_PATH, read_only=True, data_only=True)
if "Сети атрибутів" not in wb.sheetnames:
wb.close()
return set()
rows = list(wb["Сети атрибутів"].iter_rows(values_only=True))
wb.close()
if len(rows) < 2:
return set()
headers = [str(c).strip() if c else "" for c in rows[0]]
try:
sc_col = headers.index("set_code")
except ValueError:
return set()
codes = {
str(row[sc_col]).strip()
for row in rows[1:]
if len(row) > sc_col and row[sc_col]
}
print(f" set_codes в «Сети атрибутів»: {len(codes)} шт.")
return codes
except Exception as e:
print(f"⚠️ Не вдалося прочитати «Сети атрибутів»: {e}")
return set()

Нова функція — дописати нові сети атрибутів
def append_new_attr_sets(attr_sets: list[dict], new_set_codes: set[str]) -> int:
"""
Дописує в «Сети атрибутів» рядки тільки для нових set_codes.
Існуючі рядки не чіпає. Повертає кількість доданих рядків.
"""
import openpyxl as \_xl
wb = \_xl.load_workbook(OUTPUT_PATH)

    if "Сети атрибутів" not in wb.sheetnames:
        # Листа ще немає — будуємо з нуля для нових
        filtered = [s for s in attr_sets if str(s.get("code", "")) in new_set_codes]
        build_attr_sets_sheet(wb, filtered)
        wb.save(OUTPUT_PATH)
        return sum(len(s.get("attributes", [])) or 1 for s in filtered)

    ws = wb["Сети атрибутів"]
    added = 0

    for aset in attr_sets:
        sc = str(aset.get("code", ""))
        if sc not in new_set_codes:
            continue  # тільки нові

        sn    = _get_translation(aset.get("translations", []))
        attrs = aset.get("attributes", [])
        rows_data = [
            [
                sc, sn,
                a.get("code", ""),
                _get_translation(a.get("translations", [])),
                a.get("type", ""),
                a.get("isRequired", False),
                a.get("isFilter", False),
                a.get("isSystem", False),
                a.get("isModel", False),
                "",  # prom_param_name — заповнить користувач
            ]
            for a in attrs
        ] or [[sc, sn, "", "", "", "", "", "", "", ""]]

        next_row = ws.max_row + 1
        for row in rows_data:
            for ci, val in enumerate(row, 1):
                _data(ws.cell(row=next_row, column=ci, value=val),
                      fill=GRAY_FILL if ci <= 2 else None)
            next_row += 1
            added += 1

    wb.save(OUTPUT_PATH)
    print(f"   ✅ Дописано {added} рядків у «Сети атрибутів» для {len(new_set_codes)} нових set_codes.")
    return added

Оновлений options_only_mode в main()
if options_only_mode:
print("⚡ Режим: інкрементальне оновлення\n")

    # ── Крок 1: нові категорії з mappings.xlsx ──────────────────────────
    prom_categories = load_prom_categories()
    added_categories = append_new_prom_categories(prom_categories)

    if added_categories > 0:
        print(
            f"\n📌 Додано {added_categories} нових категорій у «Маппінг».\n"
            f"   Заповни epicenter_category_id одним із способів:\n"
            f"   • Автоматично: python scripts/epicenter_map_categories.py\n"
            f"   • Вручну:      колонки C (epicenter_category_id),\n"
            f"                           D (Назва категорії Епіцентру),\n"
            f"                           E (parentCode)\n"
            f"   Потім: python scripts/epicenter_export_categories.py"
        )
        return  # чекаємо поки заповнять

    # ── Крок 2: нові set_codes → дописати «Сети атрибутів» ─────────────
    existing_in_attr_sets = load_set_codes_in_attr_sets()
    new_set_codes = mapped_set_codes - existing_in_attr_sets

    if new_set_codes:
        print(f"   Нових set_codes без атрибутів: {len(new_set_codes)} → завантажуємо з API...")
        attr_sets_from_api = fetch_attribute_sets()
        added_attrs = append_new_attr_sets(attr_sets_from_api, new_set_codes)

        if added_attrs > 0:
            print(
                f"\n📌 Дописано атрибути для {len(new_set_codes)} нових категорій у «Сети атрибутів».\n"
                f"   Заповни prom_param_name (колонка J) для нових рядків.\n"
                f"   Потім: python scripts/epicenter_export_categories.py\n"
                f"   → Опції завантажаться тільки для нових категорій."
            )
            return  # чекаємо поки заповнять prom_param_name
    else:
        # Для fetch_all_options потрібен актуальний список сетів
        attr_sets_from_api = None  # буде завантажено з xlsx нижче

    # ── Крок 3: нові опції — тільки для set_codes без опцій ─────────────
    existing_with_options = load_set_codes_with_options()
    new_set_codes_for_options = mapped_set_codes - existing_with_options

    if not new_set_codes_for_options:
        print("   ✅ Опції вже завантажені для всіх категорій.")
        return

    attr_sets = attr_sets_from_api or _load_attr_sets_from_xlsx()
    option_rows = fetch_all_options(attr_sets, new_set_codes_for_options, mapped_attr_pairs)

    if not option_rows:
        return

    import openpyxl as _xl
    wb = _xl.load_workbook(OUTPUT_PATH)

    if "Опції атрибутів" in wb.sheetnames and option_rows:
        ws_opts = wb["Опції атрибутів"]
        next_row = ws_opts.max_row + 1
        for ri, row in enumerate(option_rows, next_row):
            vals = [
                row["set_code"], row["set_name_uk"], row["attr_code"],
                row["attr_name_uk"], row["attr_type"],
                row["option_code"], row["option_name_uk"], row.get("prom_option_name", ""),
            ]
            for ci, val in enumerate(vals, 1):
                fill = (GRAY_FILL if ci <= 2
                        else BLUE_FILL if ci in (6, 7)
                        else YELLOW_FILL if ci == 8
                        else None)
                _data(ws_opts.cell(row=ri, column=ci, value=val), fill=fill)
        print(f"   ✅ Дописано {len(option_rows)} рядків у «Опції атрибутів».")
    elif option_rows:
        build_options_sheet(wb, option_rows)

    wb.save(OUTPUT_PATH)
    print(f"\n✅ Оновлено: {OUTPUT_PATH}")
    return

Тепер кожен крок чітко логується і скрипт зупиняється з підказкою — не треба нічого видаляти чи думати що робити далі.
