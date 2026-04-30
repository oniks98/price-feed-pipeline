"""
Завантажує категорії Епіцентру через API V2 і генерує файл маппінгу:
  data/markets/epicenter_mappings.xlsx
  Категорії ПРОМу завантажує з mappings.xlsx

Запуск:
    python scripts/epicenter_export_categories.py

Опції атрибутів завантажуються ТІЛЬКИ для set_code, що вже заповнені
в колонці epicenter_category_id листа «Маппінг» (якщо файл вже існує).
Перший запуск — без опцій. Після заповнення маппінгу — запусти ще раз.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from pathlib import Path
from threading import Lock

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

_ID_VARIANTS   = {"ід категорії фіду", "id категорії фіду", "ід категории", "prom_category_id", "id"}
_NAME_VARIANTS = {"категорія прому", "назва категорії", "категория прому", "name", "назва"}

OPTION_TYPES    = {"select", "multiselect"}
OPTIONS_WORKERS = 8         # паралельних потоків
REQ_TIMEOUT     = (10, 30)  # (connect_sec, read_sec) — tuple, не дає зависнути
MAX_PAGES       = 200        # ліміт сторінок на атрибут (50 × ~50 = ~2500 опцій)
FUTURE_TIMEOUT  = 90        # секунд — hard kill через wait(..., timeout=)


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=2, backoff_factor=0.5, status_forcelist=(429,), allowed_methods=["GET"])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _get_translation(translations: list[dict], lang: str = "ua") -> str:
    """API використовує languageCode='ua' і field='value' (не 'uk'/'title' як в документації)."""
    for priority_lang in (lang, "ua", "uk", "ru", "en"):
        for t in translations:
            if t.get("languageCode") == priority_lang:
                val = t.get("value") or t.get("title") or ""
                if str(val).strip():
                    return str(val).strip()
    return ""


def _parse_option_name(opt: dict) -> str:
    """
    Витягує назву опції. Реальна структура API:
      translations[].value, languageCode: 'ua' або 'ru'
    """
    translations = opt.get("translations", [])
    if translations:
        for lang in ("ua", "ru", "en", "uk"):
            for t in translations:
                if t.get("languageCode") == lang:
                    val = t.get("value") or t.get("title") or ""
                    if str(val).strip():
                        return str(val).strip()
        for t in translations:
            val = t.get("value") or t.get("title") or ""
            if str(val).strip():
                return str(val).strip()

    for field in ("name", "title", "label", "value"):
        val = opt.get(field, "")
        if val and str(val).strip():
            return str(val).strip()

    return ""


# ─── Load already-mapped set_codes ───────────────────────────────────────────

def load_mapped_attr_pairs() -> set[tuple[str, str]]:
    """
    Читає лист «Сети атрибутів» і повертає set пар (set_code, attr_code)
    де prom_param_name вже заповнений.
    Тільки для цих пар будемо качати опції.
    """
    if not OUTPUT_PATH.exists():
        return set()
    try:
        import openpyxl as _xl
        wb = _xl.load_workbook(OUTPUT_PATH, read_only=True, data_only=True)
        if "Сети атрибутів" not in wb.sheetnames:
            wb.close()
            return set()
        rows = list(wb["Сети атрибутів"].iter_rows(values_only=True))
        wb.close()
        if not rows:
            return set()
        headers = [str(c).strip() if c else "" for c in rows[0]]
        try:
            sc_col   = headers.index("set_code")
            ac_col   = headers.index("attr_code")
            prom_col = headers.index("prom_param_name")
        except ValueError:
            return set()
        pairs = {
            (str(row[sc_col]).strip(), str(row[ac_col]).strip())
            for row in rows[1:]
            if len(row) > prom_col
            and row[sc_col] and row[ac_col]
            and row[prom_col] and str(row[prom_col]).strip()
        }
        print(f"   Знайдено {len(pairs)} заповнених (set_code, attr_code) пар з prom_param_name.")
        return pairs
    except Exception as e:
        print(f"⚠️  Не вдалося прочитати пари атрибутів: {e}")
        return set()


def load_mapped_set_codes() -> set[str]:
    if not OUTPUT_PATH.exists():
        return set()
    try:
        import openpyxl as _xl
        wb = _xl.load_workbook(OUTPUT_PATH, read_only=True, data_only=True)
        if "Маппінг" not in wb.sheetnames:
            wb.close()
            return set()
        rows = list(wb["Маппінг"].iter_rows(values_only=True))
        wb.close()
        if not rows:
            return set()
        headers = [str(c).strip() if c else "" for c in rows[0]]
        try:
            col = headers.index("epicenter_category_id")
        except ValueError:
            return set()
        codes = {
            str(row[col]).strip()
            for row in rows[1:]
            if len(row) > col and row[col]
        }
        print(f"   Знайдено {len(codes)} заповнених epicenter_category_id.")
        return codes
    except Exception as e:
        print(f"⚠️  Не вдалося прочитати файл: {e}")
        return set()


# ─── Prom categories ─────────────────────────────────────────────────────────

def load_prom_categories() -> dict[str, str]:
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
        headers  = [str(h).strip().lower() if h else "" for h in rows[0]]
        id_col   = next((i for i, h in enumerate(headers) if "id" in h.lower() or "ід" in h), 0)
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
                result[str(cid).strip()] = str(cname).strip()
        print(f"✅ Категорії Прому: {len(result)} шт.")
        return result
    except FileNotFoundError:
        print(f"⚠️  mappings.xlsx не знайдено: {MAPPINGS_PATH}")
        return {}
    except Exception as e:
        print(f"⚠️  Помилка mappings.xlsx: {e}")
        return {}


# ─── Epicenter API ────────────────────────────────────────────────────────────

def fetch_categories() -> list[dict]:
    print("⬇️  Категорії Епіцентру...")
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
    print(f"✅ Категорій: {len(items)}")
    return items


def fetch_attribute_sets() -> list[dict]:
    print("⬇️  Сети атрибутів...")
    session = _make_session()
    sets: list[dict] = []
    page = 1
    while True:
        try:
            resp = session.get(
                f"{BASE_URL}/attribute-sets", params={"page": page}, timeout=REQ_TIMEOUT
            )
            print(f"   attribute-sets сторінка {page} → HTTP {resp.status_code}")
            if resp.status_code == 403:
                print(f"❌ 403: {resp.text[:200]}")
                break
            data = resp.json()
        except Exception as e:
            print(f"❌ attribute-sets p{page}: {e}")
            break
        batch = data.get("items", [])
        if not batch:
            break
        sets.extend(batch)
        total = data.get("pages", 1)
        print(f"   {page}/{total}: {len(batch)} сетів")
        if page >= total:
            break
        page += 1
    print(f"✅ Сетів атрибутів: {len(sets)}")
    return sets


# ─── Options (parallel + cache + real timeout) ───────────────────────────────

def _fetch_options_one_attr(attr_code: str, set_code: str) -> list[dict]:
    """
    Завантажує всі сторінки опцій для одного attr_code.
    set_code потрібен тільки для URL — опції однакові для всіх set_code.
    Ліміт MAX_PAGES запобігає нескінченній пагінації (attr=78 має 8439 опцій).
    """
    session = _make_session()
    options: list[dict] = []
    page = 1
    while page <= MAX_PAGES:
        try:
            resp = session.get(
                f"{BASE_URL}/attribute-sets/{set_code}/attributes/{attr_code}/options",
                params={"page": page},
                timeout=REQ_TIMEOUT,
            )
            if resp.status_code in (403, 404):
                break
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            break
        batch = data.get("items", [])
        if not batch:
            break
        options.extend(batch)
        if page >= data.get("pages", 1):
            break
        page += 1

    if page > MAX_PAGES:
        print(f"      ⚠️  attr={attr_code} обрізано на {MAX_PAGES} стор. ({len(options)} опцій)")

    return options


def fetch_all_options(
    attr_sets: list[dict],
    filter_set_codes: set[str] | None,
    mapped_attr_pairs: set[tuple[str, str]],
) -> list[dict]:
    """
    Качає опції тільки для пар (set_code, attr_code) де prom_param_name вже заповнений.
    Якщо mapped_attr_pairs порожній — опції не потрібні (перший запуск або маппінг не заповнений).
    """
    if filter_set_codes is not None and not filter_set_codes:
        print(
            "\n⏭️  Опції пропущено — epicenter_category_id ще не заповнено.\n"
            "   Заповни маппінг і запусти скрипт ще раз."
        )
        return []

    if not mapped_attr_pairs:
        print(
            "\n⏭️  Опції пропущено — prom_param_name не заповнено в «Сети атрибутів».\n"
            "   Запусти map_epicenter_attributes.py і потім повтори цей скрипт."
        )
        return []

    pair_meta: dict[tuple[str, str], dict] = {}

    for aset in attr_sets:
        sc = str(aset.get("code", ""))
        if filter_set_codes is not None and sc not in filter_set_codes:
            continue
        sn = _get_translation(aset.get("translations", []))
        for attr in aset.get("attributes", []):
            atype = attr.get("type", "").lower()
            if atype not in OPTION_TYPES:
                continue
            ac = str(attr.get("code", ""))
            if (sc, ac) not in mapped_attr_pairs:
                continue
            pair_meta[(sc, ac)] = {
                "set_name": sn,
                "attr_name": _get_translation(attr.get("translations", [])),
                "attr_type": atype,
            }

    if not pair_meta:
        print("\n⚠️  Жодного атрибуту з prom_param_name не знайдено в сетах атрибутів")
        return []

    unique_ac: dict[str, str] = {}
    for (sc, ac) in pair_meta:
        if ac not in unique_ac:
            unique_ac[ac] = sc

    unique_attrs = len(unique_ac)
    print(
        f"\n⬇️  Опції: {unique_attrs} атрибутів з prom_param_name "
        f"({len(pair_meta)} пар set×attr, {OPTIONS_WORKERS} потоків)..."
    )

    options_cache: dict[str, list[dict]] = {}
    lock = Lock()
    done = [0]

    def _worker(ac: str) -> tuple[str, list[dict]]:
        return ac, _fetch_options_one_attr(ac, unique_ac[ac])

    with ThreadPoolExecutor(max_workers=OPTIONS_WORKERS) as pool:
        pending = {pool.submit(_worker, ac): ac for ac in unique_ac}
        remaining = set(pending.keys())

        while remaining:
            done_futures, remaining = wait(
                remaining, timeout=FUTURE_TIMEOUT, return_when=ALL_COMPLETED
            )
            for f in done_futures:
                ac = pending[f]
                try:
                    _, opts = f.result()
                    options_cache[ac] = opts
                except Exception as e:
                    options_cache[ac] = []
                    print(f"   ⚠️  ERROR attr={ac}: {e}")
                with lock:
                    done[0] += 1
                    print(f"   [{done[0]}/{unique_attrs}] attr={ac} → {len(options_cache[ac])} опцій")

            if remaining:
                print(f"   ⏱️  {len(remaining)} future(s) timeout — скасовуємо")
                for f in remaining:
                    ac = pending[f]
                    f.cancel()
                    options_cache[ac] = []
                    with lock:
                        done[0] += 1
                        print(f"   [{done[0]}/{unique_attrs}] ⏱️  TIMEOUT attr={ac}")
                break

    rows: list[dict] = []
    for (sc, ac), meta in pair_meta.items():
        opts = options_cache.get(ac, [])
        if not opts:
            rows.append({
                "set_code": sc, "set_name_uk": meta["set_name"],
                "attr_code": ac, "attr_name_uk": meta["attr_name"],
                "attr_type": meta["attr_type"],
                "option_code": "", "option_name_uk": "", "prom_option_name": "",
            })
        else:
            for opt in opts:
                rows.append({
                    "set_code": sc, "set_name_uk": meta["set_name"],
                    "attr_code": ac, "attr_name_uk": meta["attr_name"],
                    "attr_type": meta["attr_type"],
                    "option_code": opt.get("code", ""),
                    "option_name_uk": _parse_option_name(opt),
                    "prom_option_name": "",
                })

    print(f"✅ Опцій зібрано: {len(rows)} рядків ({len(pair_meta)} пар set×attr)")
    return rows


# ─── Styles ───────────────────────────────────────────────────────────────────

HDR_FILL    = PatternFill("solid", start_color="1F4E79", end_color="1F4E79")
HDR_FONT    = Font(bold=True, color="FFFFFF", name="Arial", size=10)
YELLOW_FILL = PatternFill("solid", start_color="FFFF99", end_color="FFFF99")
GREEN_FILL  = PatternFill("solid", start_color="E2EFDA", end_color="E2EFDA")
GRAY_FILL   = PatternFill("solid", start_color="F2F2F2", end_color="F2F2F2")
BLUE_FILL   = PatternFill("solid", start_color="DEEAF1", end_color="DEEAF1")
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


# ─── Sheet builders ───────────────────────────────────────────────────────────

def build_instructions_sheet(wb: Workbook) -> None:
    ws = wb.create_sheet("Інструкція", 0)
    ws.sheet_view.showGridLines = False
    ws.column_dimensions["A"].width = 5
    ws.column_dimensions["B"].width = 90
    lines = [
        ("title", "📋  ІНСТРУКЦІЯ З МАППІНГУ КАТЕГОРІЙ ПРОМУ → ЕПІЦЕНТР"),
        ("",      ""),
        ("step",  "КРОК 1 — Лист «Категорії Епіцентру»"),
        ("body",  "   Знайди категорію листового рівня (hasChild = False). Скопіюй її code."),
        ("",      ""),
        ("step",  "КРОК 2 — Лист «Маппінг»"),
        ("body",  "   Для кожної категорії Прому вписуй code Епіцентру у жовту колонку C."),
        ("",      ""),
        ("step",  "КРОК 3 — Запусти скрипт ще раз"),
        ("body",  "   Опції завантажуються тільки для заповнених epicenter_category_id."),
        ("",      ""),
        ("step",  "КРОК 4 — Лист «Сети атрибутів»"),
        ("body",  "   У жовтому стовпці J (prom_param_name) вписуй назву <param name='...'> з Прому."),
        ("body",  "   🔴 Червоні клітинки — isRequired=True, обов'язково заповнити!"),
        ("",      ""),
        ("step",  "КРОК 5 — Лист «Опції атрибутів»"),
        ("body",  "   option_code (F) — підставляється у XML Епіцентру."),
        ("body",  "   option_name_uk (G) — значення опції з Епіцентру."),
        ("body",  "   🟡 prom_option_name (H) — вписуй відповідне значення <param> з Прому."),
        ("",      ""),
        ("warn",  "⚠️  code = set_code = epicenter_category_id — ОДНА цифра!"),
        ("body",  f"   API: https://api.epicentrm.com.ua/swagger/ | Токен: {API_TOKEN}"),
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
    ws.cell(row=1, column=8, value="🟡 C — epicenter_category_id — заповнити вручну").font = (
        Font(bold=True, color="7F6000", name="Arial", size=9)
    )
    ws.cell(row=2, column=8, value="🟢 A, B — з фіду Прому, не змінювати").font = (
        Font(bold=True, color="375623", name="Arial", size=9)
    )
    ws.cell(row=3, column=8, value="ℹ️  epicenter_category_id = code = set_code — одна цифра!").font = (
        Font(bold=True, color="1F4E79", name="Arial", size=9)
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


def build_attr_sets_sheet(wb: Workbook, attr_sets: list[dict]) -> None:
    ws = wb.create_sheet("Сети атрибутів")
    headers    = ["set_code", "set_name_uk", "attr_code", "attr_name_uk", "attr_type",
                  "isRequired", "isFilter", "isSystem", "isModel", "prom_param_name"]
    col_widths = [30, 40, 30, 42, 16, 12, 12, 12, 12, 35]
    for ci, (h, w) in enumerate(zip(headers, col_widths), 1):
        _hdr(ws.cell(row=1, column=ci, value=h))
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.cell(
        row=1, column=12,
        value="🟡 prom_param_name (J) — назва <param name='...'> з фіду Прому | 🔴 = обов'язково заповнити",
    ).font = Font(bold=True, color="7F6000", name="Arial", size=9)

    row_idx = 2
    for aset in attr_sets:
        sc    = aset.get("code", "")
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
                "",
            ]
            for a in attrs
        ] or [[sc, sn, "", "", "", "", "", "", "", ""]]

        for row in rows_data:
            for ci, val in enumerate(row, 1):
                fill = GRAY_FILL if ci <= 2 else None
                _data(ws.cell(row=row_idx, column=ci, value=val), fill=fill)
            row_idx += 1

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"


def build_options_sheet(wb: Workbook, option_rows: list[dict]) -> None:
    ws = wb.create_sheet("Опції атрибутів")
    headers    = ["set_code", "set_name_uk", "attr_code", "attr_name_uk",
                  "attr_type", "option_code", "option_name_uk", "prom_option_name"]
    col_widths = [30, 40, 30, 42, 16, 30, 45, 45]
    for ci, (h, w) in enumerate(zip(headers, col_widths), 1):
        _hdr(ws.cell(row=1, column=ci, value=h))
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.cell(
        row=1, column=10,
        value="🟡 prom_option_name (H) — відповідне значення <param> з фіду Прому",
    ).font = Font(bold=True, color="7F6000", name="Arial", size=9)

    for ri, row in enumerate(option_rows, 2):
        vals = [
            row["set_code"], row["set_name_uk"], row["attr_code"],
            row["attr_name_uk"], row["attr_type"],
            row["option_code"], row["option_name_uk"],
            row.get("prom_option_name", ""),
        ]
        for ci, val in enumerate(vals, 1):
            if ci <= 2:
                fill = GRAY_FILL
            elif ci in (6, 7):
                fill = BLUE_FILL    # option_code і option_name_uk — з API
            elif ci == 8:
                fill = YELLOW_FILL  # prom_option_name — заповнювати вручну
            else:
                fill = None
            _data(ws.cell(row=ri, column=ci, value=val), fill=fill)

    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{len(option_rows) + 1}"


# ─── Load attr_sets from existing xlsx (for options-only mode) ─────────────────

def _load_attr_sets_from_xlsx() -> list[dict]:
    """
    Читає лист «Сети атрибутів» і реконструює структуру як fetch_attribute_sets()
    для передачі в fetch_all_options().
    Використовується в options-only режимі без звернення до API.
    """
    if not OUTPUT_PATH.exists():
        return []
    try:
        import openpyxl as _xl
        wb = _xl.load_workbook(OUTPUT_PATH, read_only=True, data_only=True)
        if "Сети атрибутів" not in wb.sheetnames:
            wb.close()
            return []
        rows = list(wb["Сети атрибутів"].iter_rows(values_only=True))
        wb.close()
        if not rows:
            return []
        headers = [str(c).strip() if c else "" for c in rows[0]]
        idx = {h: i for i, h in enumerate(headers)}

        sets: dict[str, dict] = {}
        for row in rows[1:]:
            sc = str(row[idx["set_code"]] or "").strip()
            sn = str(row[idx.get("set_name_uk", -1)] or "").strip() if "set_name_uk" in idx else ""
            ac = str(row[idx["attr_code"]] or "").strip()
            an = str(row[idx.get("attr_name_uk", -1)] or "").strip() if "attr_name_uk" in idx else ""
            at = str(row[idx.get("attr_type", -1)] or "").strip() if "attr_type" in idx else ""
            if not sc or not ac:
                continue
            if sc not in sets:
                sets[sc] = {
                    "code": sc,
                    "translations": [{"languageCode": "ua", "value": sn}],
                    "attributes": [],
                }
            sets[sc]["attributes"].append({
                "code": ac,
                "type": at,
                "translations": [{"languageCode": "ua", "value": an}],
            })

        result = list(sets.values())
        print(f"   Читано з xlsx: {len(result)} сетів атрибутів")
        return result
    except Exception as e:
        print(f"⚠️  Не вдалося зачитати сети з xlsx: {e}")
        return []


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    print("🚀 Завантаження даних Епіцентру...\n")

    mapped_attr_pairs = load_mapped_attr_pairs()
    mapped_set_codes  = load_mapped_set_codes()

    options_only_mode = (
        bool(mapped_set_codes)
        and bool(mapped_attr_pairs)
        and OUTPUT_PATH.exists()
    )

    if options_only_mode:
        print("⚡ Режим: оновлення опцій (категорії і сети атрибутів не перезавантажуються)\n")
        attr_sets   = _load_attr_sets_from_xlsx()
        option_rows = fetch_all_options(attr_sets, mapped_set_codes, mapped_attr_pairs)

        import openpyxl as _xl
        wb = _xl.load_workbook(OUTPUT_PATH)

        # Оновлюємо лист «Опції атрибутів»
        if "Опції атрибутів" in wb.sheetnames:
            del wb["Опції атрибутів"]
        if option_rows:
            build_options_sheet(wb, option_rows)
        else:
            ws = wb.create_sheet("Опції атрибутів")
            ws["A1"] = "⚠️ Заповни prom_param_name у «Сети атрибутів» і запусти скрипт ще раз."
            ws["A1"].font = Font(bold=True, color="C00000")

        wb.save(OUTPUT_PATH)
        print(f"\n✅ Оновлено лист «Опції атрибутів»: {OUTPUT_PATH}")
        return

    # Повний режим: перезавантажуємо все
    prom_categories = load_prom_categories()
    categories      = fetch_categories()
    attr_sets       = fetch_attribute_sets()
    option_rows     = fetch_all_options(attr_sets, mapped_set_codes, mapped_attr_pairs) if attr_sets else []

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

    if attr_sets:
        build_attr_sets_sheet(wb, attr_sets)
    else:
        ws = wb.create_sheet("Сети атрибутів")
        ws["A1"] = "⚠️ Не завантажено."
        ws["A1"].font = Font(bold=True, color="C00000")

    if option_rows:
        build_options_sheet(wb, option_rows)
    else:
        ws = wb.create_sheet("Опції атрибутів")
        ws["A1"] = "⚠️ Заповни epicenter_category_id у «Маппінг» і запусти скрипт ще раз."
        ws["A1"].font = Font(bold=True, color="C00000")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    wb.save(OUTPUT_PATH)
    print(f"\n✅ Збережено: {OUTPUT_PATH}")
    print(f"   Листи: {wb.sheetnames}")
    if not mapped_set_codes:
        print("\n📌 Заповни колонку C (epicenter_category_id) → запусти скрипт ще раз")
    elif not mapped_attr_pairs:
        print("\n📌 Запусти map_epicenter_attributes.py → заповни prom_param_name → запусти цей скрипт ще раз")
    else:
        print("\n📌 Опції завантажено в повному режимі")


if __name__ == "__main__":
    main()
