"""
epicenter_export_categories.py
-------------------------------
КРОК 1 з 5 пайплайну маппінгу Prom → Epicenter.

Що робить:
  1. Читає категорії Прому з mappings.xlsx.
  2. Завантажує з API Epicenter тільки ті категорії, що є в маппінгу Прому.
  3. Генерує (або оновлює) epicenter_mappings.xlsx:
       • Лист «Маппінг»             — категорії Прому + колонки для заповнення
       • Лист «Категорії Епіцентру» — повний довідник категорій Epicenter

Інкрементальна логіка:
  Якщо epicenter_mappings.xlsx вже існує — дописує тільки нові категорії Прому
  в лист «Маппінг», старі рядки не чіпає.

Наступний крок:
  Заповни epicenter_category_id у «Маппінг» одним зі способів:
    • Автоматично: python scripts/epicenter_map_categories.py
    • Вручну:      колонки C (epicenter_category_id), D (Назва), E (parentCode)
  Потім → python scripts/epicenter_export_attr_sets.py

Запуск:
    python scripts/epicenter_export_categories.py
"""

from __future__ import annotations

from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

API_TOKEN = "5a6489d1a5c48c9d174bd31f2a0a8fd0"
BASE_URL  = "https://api.epicentrm.com.ua/v2/pim"
HEADERS   = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

ROOT          = Path(__file__).parents[1]
OUTPUT_PATH   = ROOT / "data" / "markets" / "epicenter_mappings.xlsx"
MAPPINGS_PATH = ROOT / "data" / "markets" / "mappings.xlsx"

REQ_TIMEOUT = (10, 30)


# ─── Session ──────────────────────────────────────────────────────────────────

def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=0.5, status_forcelist=(429,), allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _norm_id(val: object) -> str:
    """Нормалізує ідентифікатор: '123.0' → '123'."""
    if val is None:
        return ""
    s = str(val).strip()
    if "." in s:
        try:
            f = float(s)
            if f == int(f):
                return str(int(f))
        except (ValueError, OverflowError):
            pass
    return s


def _get_translation(translations: list[dict], lang: str = "ua") -> str:
    for priority_lang in (lang, "ua", "uk", "ru", "en"):
        for t in translations:
            if t.get("languageCode") == priority_lang:
                val = t.get("value") or t.get("title") or ""
                if str(val).strip():
                    return str(val).strip()
    return ""


# ─── Styles ───────────────────────────────────────────────────────────────────

HDR_FILL    = PatternFill("solid", start_color="1F4E79", end_color="1F4E79")
HDR_FONT    = Font(bold=True, color="FFFFFF", name="Arial", size=10)
YELLOW_FILL = PatternFill("solid", start_color="FFFF99", end_color="FFFF99")
GREEN_FILL  = PatternFill("solid", start_color="E2EFDA", end_color="E2EFDA")
THIN_BORDER = Border(
    left=Side(style="thin"), right=Side(style="thin"),
    top=Side(style="thin"),  bottom=Side(style="thin"),
)
CENTER = Alignment(horizontal="center", vertical="center", wrap_text=True)
LEFT   = Alignment(horizontal="left",   vertical="center", wrap_text=True)


def _hdr(cell, fill=HDR_FILL) -> None:
    cell.font = HDR_FONT
    cell.fill = fill
    cell.alignment = CENTER
    cell.border = THIN_BORDER


def _data(cell, fill=None) -> None:
    cell.font = Font(name="Arial", size=9)
    cell.alignment = LEFT
    cell.border = THIN_BORDER
    if fill:
        cell.fill = fill


# ─── Readers ──────────────────────────────────────────────────────────────────

def load_prom_categories() -> dict[str, str]:
    """Зчитує категорії Прому з mappings.xlsx → {prom_category_id: name}."""
    try:
        import openpyxl as _xl
        wb = _xl.load_workbook(MAPPINGS_PATH, read_only=True, data_only=True)
        sheet_name = next((n for n in wb.sheetnames if n.strip().startswith("Категорія")), None)
        if not sheet_name:
            print(f"⚠️  Лист 'Категорія+' не знайдено. Доступні: {wb.sheetnames}")
            wb.close()
            return {}
        rows = list(wb[sheet_name].iter_rows(values_only=True))
        wb.close()
        if not rows:
            return {}
        headers = [str(h).strip().lower() if h else "" for h in rows[0]]
        _id_exact = next((i for i, h in enumerate(headers) if h in ("prom_category_id", "id", "ід")), None)
        id_col = _id_exact if _id_exact is not None else next(
            (i for i, h in enumerate(headers) if "id" in h or "ід" in h), 0
        )
        name_col = next(
            (i for i, h in enumerate(headers)
             if i != id_col and ("категорі" in h or "назва" in h)), 1
        )
        result: dict[str, str] = {}
        for row in rows[1:]:
            if len(row) <= max(id_col, name_col):
                continue
            cid, cname = row[id_col], row[name_col]
            if cid and cname:
                result[_norm_id(cid)] = str(cname).strip()
        print(f"✅ Категорії Прому: {len(result)} шт.")
        return result
    except FileNotFoundError:
        print(f"⚠️  mappings.xlsx не знайдено: {MAPPINGS_PATH}")
        return {}
    except Exception as e:
        print(f"⚠️  Помилка mappings.xlsx: {e}")
        return {}


# ─── API ──────────────────────────────────────────────────────────────────────

def fetch_categories() -> list[dict]:
    print("⬇️  Категорії Епіцентру (всі)...")
    session = _make_session()
    items: list[dict] = []
    page = 1
    while True:
        try:
            data = session.get(
                f"{BASE_URL}/categories", params={"page": page}, timeout=REQ_TIMEOUT
            ).json()
        except Exception as e:
            print(f"❌ categories p{page}: {e}")
            break
        batch = data.get("items", [])
        if not batch:
            break
        items.extend(batch)
        total = data.get("pages", 1)
        print(f"   {page}/{total}: {len(batch)} категорій")
        if page >= total:
            break
        page += 1
    print(f"✅ Категорій всього: {len(items)}")
    return items


# ─── Incremental writer ───────────────────────────────────────────────────────

def append_new_prom_categories(prom_categories: dict[str, str]) -> int:
    """Дописує в «Маппінг» тільки нові категорії Прому. Повертає кількість доданих."""
    import openpyxl as _xl
    wb = _xl.load_workbook(OUTPUT_PATH)
    if "Маппінг" not in wb.sheetnames:
        wb.close()
        return 0
    ws = wb["Маппінг"]
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        wb.close()
        return 0
    headers = [str(c).strip() if c else "" for c in rows[0]]
    try:
        id_col = headers.index("prom_category_id")
    except ValueError:
        wb.close()
        return 0

    existing_ids = {
        _norm_id(row[id_col])
        for row in rows[1:]
        if len(row) > id_col and row[id_col]
    }
    new_items = {k: v for k, v in prom_categories.items() if _norm_id(k) not in existing_ids}

    if not new_items:
        print("   ✅ Нових категорій у «Маппінг» немає.")
        wb.close()
        return 0

    next_row = ws.max_row + 1
    for pid, pname in new_items.items():
        for ci, val in enumerate([pid, pname, "", "", "", ""], 1):
            _data(
                ws.cell(row=next_row, column=ci, value=val),
                fill=GREEN_FILL if ci <= 2 else YELLOW_FILL if ci == 3 else None,
            )
        next_row += 1

    wb.save(OUTPUT_PATH)
    wb.close()
    print(f"   ✅ Додано {len(new_items)} нових категорій у «Маппінг».")
    return len(new_items)


# ─── Sheet builders ───────────────────────────────────────────────────────────

def build_instructions_sheet(wb: Workbook) -> None:
    ws = wb.create_sheet("Інструкція", 0)
    ws.sheet_view.showGridLines = False
    ws.column_dimensions["A"].width = 5
    ws.column_dimensions["B"].width = 100

    lines = [
        ("title", "📋  ІНСТРУКЦІЯ З МАППІНГУ КАТЕГОРІЙ ПРОМУ → ЕПІЦЕНТР"),
        ("",      ""),
        ("step",  "КРОК 1 — Генерація файлу маппінгу"),
        ("body",  "   Запуск: python scripts/epicenter_export_categories.py"),
        ("body",  "   Результат: з'являються листи «Маппінг» і «Категорії Епіцентру»."),
        ("body",  "   У «Маппінг» завантажено всі категорії з вашого фіду Прому (колонки A, B)."),
        ("",      ""),
        ("step",  "КРОК 2 — Заповнення маппінгу категорій"),
        ("body",  "   Варіант А (автоматично): python scripts/epicenter_map_categories.py"),
        ("body",  "   Варіант Б (вручну): у «Маппінг» заповни жовті колонки:"),
        ("body",  "     C — epicenter_category_id  (code з листа «Категорії Епіцентру»)"),
        ("body",  "     D — Назва категорії Епіцентру"),
        ("body",  "     E — parentCode"),
        ("body",  "   ⚠️  epicenter_category_id = code = set_code — одне й те саме число!"),
        ("",      ""),
        ("step",  "КРОК 3 — Завантаження сетів атрибутів"),
        ("body",  "   Запуск: python scripts/epicenter_export_attr_sets.py"),
        ("body",  "   Результат: лист «Сети атрибутів» — атрибути тільки для заповнених категорій."),
        ("",      ""),
        ("step",  "КРОК 4 — Заповнення prom_param_name"),
        ("body",  "   Варіант А (автоматично): python scripts/epicenter_map_attributes.py"),
        ("body",  "   Варіант Б (вручну): у «Сети атрибутів» заповни колонку J (prom_param_name)."),
        ("body",  "   🔴 Червоні клітинки — isRequired=True, обов'язково заповнити!"),
        ("",      ""),
        ("step",  "КРОК 5 — Завантаження опцій атрибутів"),
        ("body",  "   Запуск: python scripts/epicenter_export_attr_options.py"),
        ("body",  "   Результат: лист «Опції атрибутів» — тільки для атрибутів з заповненим prom_param_name."),
        ("",      ""),
        ("step",  "КРОК 6 — Заповнення prom_option_name"),
        ("body",  "   Заповни колонку H (prom_option_name) вручну або напиши скрипт маппінгу."),
        ("body",  "   option_code (F) підставляється у XML Епіцентру."),
        ("",      ""),
        ("warn",  f"   API: https://api.epicentrm.com.ua/swagger/ | Токен: {API_TOKEN}"),
    ]

    for ri, (kind, text) in enumerate(lines, 1):
        ws.row_dimensions[ri].height = 18
        cell = ws.cell(row=ri, column=2, value=text)
        if kind == "title":
            cell.font = Font(bold=True, size=14, color="1F4E79", name="Arial")
        elif kind == "step":
            cell.font = Font(bold=True, size=11, color="2E75B6", name="Arial")
        elif kind == "warn":
            cell.font = Font(bold=True, size=10, color="C00000", name="Arial")
        else:
            cell.font = Font(size=10, name="Arial")
        cell.alignment = LEFT


def build_mapping_sheet(wb: Workbook, prom_categories: dict[str, str]) -> None:
    ws = wb.create_sheet("Маппінг")
    headers    = ["prom_category_id", "Категорія Прому", "epicenter_category_id",
                  "Назва категорії Епіцентру", "parentCode", "Коментар / Примітка"]
    col_widths = [22, 55, 25, 45, 20, 35]
    for ci, (h, w) in enumerate(zip(headers, col_widths), 1):
        _hdr(ws.cell(row=1, column=ci, value=h))
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.row_dimensions[1].height = 30
    for ri, (pid, pname) in enumerate(prom_categories.items(), 2):
        for ci, val in enumerate([pid, pname, "", "", "", ""], 1):
            _data(
                ws.cell(row=ri, column=ci, value=val),
                fill=GREEN_FILL if ci <= 2 else YELLOW_FILL if ci == 3 else None,
            )
    ws.cell(row=1, column=8, value="🟡 C — epicenter_category_id — заповнити (крок 2)").font = (
        Font(bold=True, color="7F6000", name="Arial", size=9)
    )
    ws.cell(row=2, column=8, value="🟢 A, B — з фіду Прому, не змінювати").font = (
        Font(bold=True, color="375623", name="Arial", size=9)
    )
    ws.freeze_panes = "A2"


def build_categories_sheet(wb: Workbook, categories: list[dict]) -> None:
    ws = wb.create_sheet("Категорії Епіцентру")
    for ci, (h, w) in enumerate(
        zip(["code", "name_uk", "parentCode", "hasChild"], [30, 50, 30, 12]), 1
    ):
        _hdr(ws.cell(row=1, column=ci, value=h))
        ws.column_dimensions[get_column_letter(ci)].width = w
    for ri, cat in enumerate(categories, 2):
        for ci, val in enumerate(
            [
                cat.get("code", ""),
                _get_translation(cat.get("translations", [])),
                cat.get("parentCode", ""),
                cat.get("hasChild", ""),
            ],
            1,
        ):
            _data(ws.cell(row=ri, column=ci, value=val))
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:D{len(categories) + 1}"


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("🚀 epicenter_export_categories.py — КРОК 1\n")

    prom_categories = load_prom_categories()
    if not prom_categories:
        print("❌ Категорії Прому не знайдено. Перевір mappings.xlsx.")
        return

    # ── Інкрементальний режим ─────────────────────────────────────────────────
    if OUTPUT_PATH.exists():
        print("⚡ Файл існує — дописуємо нові категорії Прому у «Маппінг».\n")
        added = append_new_prom_categories(prom_categories)
        if added > 0:
            print(
                f"\n📌 Додано {added} нових категорій.\n"
                f"   Заповни epicenter_category_id (крок 2) і запусти наступний скрипт:\n"
                f"   → python scripts/epicenter_export_attr_sets.py"
            )
        else:
            print(
                "\n✅ Всі категорії вже є в «Маппінг».\n"
                "   Наступний крок → python scripts/epicenter_export_attr_sets.py"
            )
        return

    # ── Перший запуск — створюємо файл ───────────────────────────────────────
    print("📄 Перший запуск — створюємо epicenter_mappings.xlsx\n")
    categories = fetch_categories()

    wb = Workbook()
    wb.remove(wb.active)
    build_instructions_sheet(wb)
    build_mapping_sheet(wb, prom_categories)

    if categories:
        build_categories_sheet(wb, categories)
    else:
        ws = wb.create_sheet("Категорії Епіцентру")
        ws["A1"] = "⚠️ Не завантажено. Перевір токен."
        ws["A1"].font = Font(bold=True, color="C00000")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    wb.save(OUTPUT_PATH)
    print(f"\n✅ Збережено: {OUTPUT_PATH}")
    print(f"   Листи: {wb.sheetnames}")
    print(
        "\n📌 КРОК 2: Заповни epicenter_category_id у «Маппінг»:\n"
        "   • Автоматично: python scripts/epicenter_map_categories.py\n"
        "   • Вручну:      колонки C, D, E у листі «Маппінг»\n"
        "   Потім → python scripts/epicenter_export_attr_sets.py"
    )


if __name__ == "__main__":
    main()
