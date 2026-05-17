"""
epicenter_export_attr_options.py
---------------------------------
КРОК 5 з 5 пайплайну маппінгу Prom → Epicenter.

Що робить:
  Завантажує з API опції атрибутів Epicenter і записує в лист «Опції атрибутів»
  ТІЛЬКИ для тих пар (set_code, attr_code), у яких isRequired = TRUE
  у листі «Сети атрибутів» — і тільки для set_codes з «Маппінгу».

Передумова:
  • Лист «Маппінг» — заповнені epicenter_category_id.
  • Лист «Сети атрибутів» — наявні рядки з isRequired = TRUE.

Інкрементальна логіка:
  Якщо «Опції атрибутів» вже існує — дописує тільки нові set_codes,
  існуючі рядки не чіпає.

Наступний крок (КРОК 6):
  Заповни prom_option_name у «Опції атрибутів» (колонка H) вручну або скриптом.

Запуск:
    python scripts/epicenter_export_attr_options.py
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock
import time

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

ROOT        = Path(__file__).parents[1]
OUTPUT_PATH = ROOT / "data" / "markets" / "epicenter_mappings.xlsx"

OPTION_TYPES     = {"select", "multiselect"}   # мають опції в API → завантажуємо
NON_OPTION_TYPES = {"float", "int", "text", "string"}  # числові/текстові → рядок без опцій
# Типи без опцій і без маппінгу — пишемо рядок-заглушку (як NON_OPTION_TYPES):
# boolean, date, datetime, color, image, file, price, range, richtext тощо
# Доповнюй якщо API поверне нові типи (лог покаже їх при запуску).
EXTRA_NON_OPTION_TYPES = {"boolean", "bool", "date", "datetime", "color",
                          "image", "file", "price", "range",
                          "richtext", "rich_text", "textarea", "array"}
ALL_NON_OPTION_TYPES   = NON_OPTION_TYPES | EXTRA_NON_OPTION_TYPES
ALL_KNOWN_TYPES        = OPTION_TYPES | ALL_NON_OPTION_TYPES

OPTIONS_WORKERS  = 8
REQ_TIMEOUT      = (10, 30)
MAX_PAGES        = 2000  # safety cap; brand ~62K опцій / 50 = ~1250 стор.
HUGE_ATTR_ROWS   = 500   # attrs з більше опцій → пишемо без styling (~1M Cell-об'єктів)

# Ключова зміна архітектури:
# Опції пишуться по УНІКАЛЬНОМУ attr_code, НЕ по парі (set_code, attr_code).
# set_codes і prom_params зберігаються через кому в окремих колонках.
# Це скорочує кількість рядків з ~1.3M до ~10K.
#
# Типи атрибутів:
#   select / multiselect  → завантажуємо опції з API (N рядків на attr_code)
#   float / int / text / string → без API, один рядок-заглушка (значення = дані товару)
#
# needs_default у рядку = True якщо хоча б один set_code використовує цей attr
# без заповненого prom_param_name (червона клітинка в «Сети атрибутів»).
# default_option_code — заповнює юзер вручну для needs_default=True рядків select/multiselect.


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


def _parse_option_name(opt: dict) -> str:
    """Витягує назву опції з translations або fallback-полів."""
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


# ─── Styles ───────────────────────────────────────────────────────────────────

HDR_FILL     = PatternFill("solid", start_color="1F4E79", end_color="1F4E79")
HDR_FONT     = Font(bold=True, color="FFFFFF", name="Arial", size=10)
ORANGE_FILL  = PatternFill("solid", start_color="FCE4D6", end_color="FCE4D6")  # needs_default рядки
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
    if fill:
        cell.fill = fill


# ─── Readers ──────────────────────────────────────────────────────────────────

def load_mapped_set_codes() -> set[str]:
    """Повертає заповнені epicenter_category_id з листа «Маппінг»."""
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
            _norm_id(row[col])
            for row in rows[1:]
            if len(row) > col and row[col]
        }
        print(f"   Знайдено {len(codes)} epicenter_category_id у «Маппінг».")
        return codes
    except Exception as e:
        print(f"⚠️  Не вдалося прочитати «Маппінг»: {e}")
        return set()


def load_mapped_attr_pairs(filter_set_codes: set[str]) -> set[tuple[str, str]]:
    """
    [НЕ ВИКОРИСТОВУЄТЬСЯ] Залишено для зворотної сумісності.
    Читає «Сети атрибутів» і повертає пари (set_code, attr_code),
    де prom_param_name заповнений І set_code є в filter_set_codes.
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
            (_norm_id(row[sc_col]), _norm_id(row[ac_col]))
            for row in rows[1:]
            if len(row) > prom_col
            and row[sc_col] and row[ac_col]
            and row[prom_col] and str(row[prom_col]).strip()
            and _norm_id(row[sc_col]) in filter_set_codes
        }
        print(f"   Знайдено {len(pairs)} пар (set_code, attr_code) з prom_param_name.")
        return pairs
    except Exception as e:
        print(f"⚠️  Не вдалося прочитати «Сети атрибутів»: {e}")
        return set()


def load_attr_set_meta(filter_set_codes: set[str]) -> dict[tuple[str, str], dict]:
    """
    Читає «Сети атрибутів» і повертає метадані для пар (set_code, attr_code).
    Включає пару ТІЛЬКИ якщо isRequired = TRUE — і set_code є в filter_set_codes.

    Типи атрибутів:
      select / multiselect  → опції завантажуються з API
      float / int / text / string → рядок без опцій (значення береться з товару напряму)

    Поле `needs_default`:
      True  — якщо prom_param_name заповнений (є маппінг, заповнюємо prom_param_name)
      False — якщо prom_param_name порожній (немає маппінгу Prom→Epicenter)
    """
    if not OUTPUT_PATH.exists():
        return {}
    try:
        import openpyxl as _xl
        wb = _xl.load_workbook(OUTPUT_PATH, read_only=True, data_only=True)
        if "Сети атрибутів" not in wb.sheetnames:
            wb.close()
            return {}
        rows = list(wb["Сети атрибутів"].iter_rows(values_only=True))
        wb.close()
        if not rows:
            return {}
        headers = [str(c).strip() if c else "" for c in rows[0]]
        idx = {h: i for i, h in enumerate(headers)}
        required_cols = {"set_code", "set_name_uk", "attr_code", "attr_name_uk",
                         "attr_type", "isRequired", "prom_param_name"}
        if not required_cols.issubset(idx):
            missing = required_cols - idx.keys()
            print(f"⚠️  Відсутні колонки в «Сети атрибутів»: {missing}")
            return {}

        meta: dict[tuple[str, str], dict] = {}
        skipped_no_match: dict[str, int] = {}  # type → count

        for row in rows[1:]:
            sc = _norm_id(row[idx["set_code"]])
            ac = _norm_id(row[idx["attr_code"]])
            if not sc or not ac or sc not in filter_set_codes:
                continue

            atype = str(row[idx["attr_type"]] or "").strip().lower()
            if atype not in ALL_KNOWN_TYPES:
                skipped_no_match[atype or "(порожній)"] = skipped_no_match.get(atype or "(порожній)", 0) + 1
                continue

            prom = str(row[idx["prom_param_name"]] or "").strip() \
                if len(row) > idx["prom_param_name"] else ""
            is_required = str(row[idx["isRequired"]] or "").strip().upper() in ("TRUE", "1", "YES")

            # Включаємо ТІЛЬКИ якщо атрибут обов'язковий (isRequired = TRUE)
            if not is_required:
                continue

            # Визначаємо фактичну групу: select/multiselect або все інші (без опцій)
            effective_type = atype if atype in OPTION_TYPES else "__no_options__"

            meta[(sc, ac)] = {
                "set_name":      str(row[idx["set_name_uk"]] or "").strip(),
                "attr_name":     str(row[idx["attr_name_uk"]] or "").strip(),
                "attr_type":     atype,
                "effective_type": effective_type,
                "prom_param":    prom,
                "needs_default": not bool(prom),
            }

        with_opts     = sum(1 for m in meta.values() if m["attr_type"] in OPTION_TYPES)
        no_opts       = sum(1 for m in meta.values() if m["attr_type"] not in OPTION_TYPES)
        needs_default = sum(1 for m in meta.values() if m["needs_default"])      # немає prom → потрібен default_option_code
        has_prom      = sum(1 for m in meta.values() if not m["needs_default"])  # є prom → дефолт не критичний
        print(
            f"   Пар isRequired=TRUE для обробки: {len(meta)} шт.\n"
            f"   • select/multiselect (завантажуємо опції з API): {with_opts}\n"
            f"   • float/int/text/string/ін. (рядок без опцій):  {no_opts}\n"
            f"   • без prom_param_name (needs_default=TRUE):      {needs_default}\n"
            f"   • з prom_param_name (needs_default=FALSE):       {has_prom}"
        )
        if skipped_no_match:
            total_skipped = sum(skipped_no_match.values())
            breakdown = ", ".join(
                f"{t!r}×{n}" for t, n in sorted(skipped_no_match.items(), key=lambda x: -x[1])
            )
            print(
                f"   ⚠️  Пропущено (невідомий тип атрибута): {total_skipped} рядків\n"
                f"       Типи: {breakdown}\n"
                f"       → Додай в EXTRA_NON_OPTION_TYPES або OPTION_TYPES у верхній частині скрипта."
            )
        return meta
    except Exception as e:
        print(f"⚠️  Не вдалося прочитати метадані атрибутів: {e}")
        return {}


def load_set_codes_with_options() -> set[str]:
    """
    Повертає set_codes, для яких вже є рядки в «Опції атрибутів».
    Нова схема: set_codes зберігаються через кому в колонці «set_codes».
    """
    if not OUTPUT_PATH.exists():
        return set()
    try:
        import openpyxl as _xl
        wb = _xl.load_workbook(OUTPUT_PATH, read_only=True, data_only=True)
        if "Опції атрибутів" not in wb.sheetnames:
            wb.close()
            return set()
        rows = list(wb["Опції атрибутів"].iter_rows(values_only=True))
        wb.close()
        if len(rows) < 2:
            return set()
        headers = [str(c).strip() if c else "" for c in rows[0]]
        try:
            sc_col = headers.index("set_codes")
        except ValueError:
            return set()
        codes: set[str] = set()
        for row in rows[1:]:
            if len(row) > sc_col and row[sc_col]:
                for sc in str(row[sc_col]).split(","):
                    sc = sc.strip()
                    if sc:
                        codes.add(sc)
        print(f"   set_codes вже з опціями: {len(codes)} шт.")
        return codes
    except Exception as e:
        print(f"⚠️  Не вдалося прочитати «Опції атрибутів»: {e}")
        return set()


def load_existing_option_attr_codes() -> set[str]:
    """
    Повертає attr_codes, які вже присутні в «Опції атрибутів».

    Використовується для розмежування:
      • існуючий attr_code → тільки дописуємо нові set_codes у стовпець
      • новий attr_code    → малюємо нові рядки (завантажуємо опції з API)

    Це вирішує проблему масового дублювання рядків для загальних атрибутів
    (brand, weight, ratio тощо) при додаванні нових категорій.
    """
    if not OUTPUT_PATH.exists():
        return set()
    try:
        import openpyxl as _xl
        wb = _xl.load_workbook(OUTPUT_PATH, read_only=True, data_only=True)
        if "Опції атрибутів" not in wb.sheetnames:
            wb.close()
            return set()
        rows = list(wb["Опції атрибутів"].iter_rows(values_only=True))
        wb.close()
        if len(rows) < 2:
            return set()
        headers = [str(c).strip() if c else "" for c in rows[0]]
        try:
            ac_col = headers.index("attr_code")
        except ValueError:
            return set()
        codes = {
            str(row[ac_col]).strip()
            for row in rows[1:]
            if len(row) > ac_col and row[ac_col]
        }
        print(f"   Існуючих attr_codes у «Опції атрибутів»: {len(codes)} шт.")
        return codes
    except Exception as e:
        print(f"⚠️  Не вдалося прочитати attr_codes з «Опції атрибутів»: {e}")
        return set()


# ─── API ──────────────────────────────────────────────────────────────────────

def _try_fetch_options(
    session: requests.Session, attr_code: str, set_code: str
) -> list[dict] | None:
    """
    Завантажує всі сторінки опцій для пари (attr_code, set_code).
    Повертає None якщо set_code повернув 403/404 або мережеву помилку
    → викликач спробує наступний set_code.
    Повертає [] якщо відповідь валідна але опцій немає.
    """
    options: list[dict] = []
    total_pages = 1
    page = 1
    while page <= MAX_PAGES:
        try:
            resp = session.get(
                f"{BASE_URL}/attribute-sets/{set_code}/attributes/{attr_code}/options",
                params={"page": page},
                timeout=REQ_TIMEOUT,
            )
            if resp.status_code in (403, 404):
                return None  # цей set_code не дає доступу — спробуємо наступний
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            return None  # мережева помилка — спробуємо наступний set_code
        batch = data.get("items", [])
        if not batch:
            break
        options.extend(batch)
        total_pages = data.get("pages", 1)
        if page % 50 == 0:
            print(f"      attr={attr_code}: стор. {page}/{total_pages} ({len(options)} опцій)...")
        if page >= total_pages:
            break
        page += 1
    if page > MAX_PAGES:
        print(
            f"      ⚠️  attr={attr_code} досягнуто safety cap {MAX_PAGES} стор."
            f" ({len(options)} опцій) — збільш MAX_PAGES"
        )
    return options


def _fetch_options_one_attr(attr_code: str, set_codes: list[str]) -> list[dict]:
    """
    Завантажує всі опції для attr_code, перебираючи set_codes по порядку.
    Зупиняється на першому set_code, який повернув валідну відповідь (навіть []).

    Це вирішує проблему: перший set_code може повертати 403/404 для певних
    атрибутів, тоді як інший set_code дає повноцінний результат.
    """
    session = _make_session()
    for set_code in set_codes:
        result = _try_fetch_options(session, attr_code, set_code)
        if result is not None:
            return result
    return []


# ─── Attr aggregation ────────────────────────────────────────────────────────────────────

class _AttrAgg:
    """Агрегат по attr_code: збирає set_codes і prom_params через кому."""
    __slots__ = ("first_set", "attr_name", "attr_type",
                 "set_codes", "prom_params", "needs_default")

    def __init__(self, sc: str, meta: dict) -> None:
        self.first_set     = sc
        self.attr_name     = meta["attr_name"]
        self.attr_type     = meta["attr_type"]
        self.set_codes:    list[str] = [sc]
        self.prom_params:  list[str] = [meta["prom_param"]] if meta["prom_param"] else []
        self.needs_default = meta["needs_default"]

    def merge(self, sc: str, meta: dict) -> None:
        if sc not in self.set_codes:
            self.set_codes.append(sc)
        if meta["prom_param"] and meta["prom_param"] not in self.prom_params:
            self.prom_params.append(meta["prom_param"])
        if meta["needs_default"]:
            self.needs_default = True


def _build_agg_dicts(
    pair_meta: dict[tuple[str, str], dict],
) -> tuple[dict[str, _AttrAgg], dict[str, _AttrAgg]]:
    """Розбиває pair_meta на select/multiselect та всі інші."""
    ac_agg:   dict[str, _AttrAgg] = {}
    nopt_agg: dict[str, _AttrAgg] = {}
    for (sc, ac), meta in pair_meta.items():
        target = ac_agg if meta["attr_type"] in OPTION_TYPES else nopt_agg
        if ac not in target:
            target[ac] = _AttrAgg(sc, meta)
        else:
            target[ac].merge(sc, meta)
    return ac_agg, nopt_agg


# ─── Streaming writer helpers ───────────────────────────────────────────────────────

HEADERS_OPTIONS    = ["attr_code", "attr_name_uk", "attr_type",
                      "option_code", "option_name_uk", "prom_option_name",
                      "needs_default", "default_option_code",
                      "set_codes", "prom_param_name"]
COL_WIDTHS_OPTIONS = [30, 42, 16, 30, 45, 45, 14, 30, 60, 60]


def _row_vals(ac: str, agg: _AttrAgg, opt: dict | None) -> list:
    """Будує список значень для одного рядка (порядок = HEADERS_OPTIONS)."""
    return [
        ac,
        agg.attr_name,
        agg.attr_type,
        opt.get("code", "") if opt else "",
        _parse_option_name(opt) if opt else "",
        "",               # prom_option_name — заповнює юзер
        agg.needs_default,
        "",               # default_option_code — заповнює юзер
        ", ".join(agg.set_codes),
        ", ".join(agg.prom_params),
    ]


def _write_row_to_ws(ws, ri: int, vals: list) -> None:
    """Пише один рядок у worksheet."""
    needs_default: bool = vals[6]
    for ci, val in enumerate(vals, 1):
        cell = ws.cell(row=ri, column=ci, value=val)
        if needs_default:
            cell.fill = ORANGE_FILL
        cell.font      = Font(name="Arial", size=9)
        cell.alignment = LEFT


def _setup_options_sheet(wb) -> tuple:
    """Повертає (ws, first_data_row). Створює лист якщо не існує."""
    if "Опції атрибутів" not in wb.sheetnames:
        ws = wb.create_sheet("Опції атрибутів")
        for ci, (h, w) in enumerate(zip(HEADERS_OPTIONS, COL_WIDTHS_OPTIONS), 1):
            _hdr(ws.cell(row=1, column=ci, value=h))
            ws.column_dimensions[get_column_letter(ci)].width = w
        ws.freeze_panes = "A2"
        return ws, 2
    ws = wb["Опції атрибутів"]
    return ws, ws.max_row + 1


def _update_existing_attr_set_codes(
    ws,
    update_agg: dict[str, "_AttrAgg"],
) -> int:
    """
    Для attr_codes, які вже є в листі: дописує нові set_codes і prom_params
    до кожного рядка цього attr_code. Нові рядки НЕ створюються.

    Логіка злиття:
      • set_codes:      додаємо нові, порядок оригінальних зберігаємо
      • prom_param_name: аналогічно
      • needs_default:  True → False ніколи не міняємо; False → True якщо новий
                        set_code потребує дефолту

    Повертає кількість унікальних attr_code, в яких оновлено хоча б один рядок.
    """
    if not update_agg:
        return 0

    # Знаходимо індекси колонок з заголовку (1-based)
    headers = [
        str(ws.cell(row=1, column=c).value or "").strip()
        for c in range(1, ws.max_column + 1)
    ]
    try:
        ac_col  = headers.index("attr_code")      + 1
        sc_col  = headers.index("set_codes")      + 1
        pp_col  = headers.index("prom_param_name") + 1
        nd_col  = headers.index("needs_default")  + 1
    except ValueError as exc:
        print(f"⚠️  Не вдалося знайти колонку в «Опції атрибутів»: {exc}")
        return 0

    # Попередньо обчислюємо нові значення для кожного attr_code
    new_sc_by_ac:  dict[str, list[str]] = {ac: agg.set_codes   for ac, agg in update_agg.items()}
    new_pp_by_ac:  dict[str, list[str]] = {ac: agg.prom_params for ac, agg in update_agg.items()}
    nd_upgrade:    set[str]             = {ac for ac, agg in update_agg.items() if agg.needs_default}

    touched: set[str] = set()

    for row_idx in range(2, ws.max_row + 1):
        ac = str(ws.cell(row=row_idx, column=ac_col).value or "").strip()
        if ac not in update_agg:
            continue

        # set_codes: зберігаємо порядок, без дублів
        existing_sc = str(ws.cell(row=row_idx, column=sc_col).value or "")
        existing_sc_list = [s.strip() for s in existing_sc.split(",") if s.strip()]
        existing_sc_set  = set(existing_sc_list)
        appended_sc = [sc for sc in new_sc_by_ac[ac] if sc not in existing_sc_set]
        if appended_sc:
            ws.cell(row=row_idx, column=sc_col).value = ", ".join(existing_sc_list + appended_sc)

        # prom_param_name: аналогічно
        existing_pp = str(ws.cell(row=row_idx, column=pp_col).value or "")
        existing_pp_list = [p.strip() for p in existing_pp.split(",") if p.strip()]
        existing_pp_set  = set(existing_pp_list)
        appended_pp = [p for p in new_pp_by_ac[ac] if p not in existing_pp_set]
        if appended_pp:
            ws.cell(row=row_idx, column=pp_col).value = ", ".join(existing_pp_list + appended_pp)

        # needs_default: тільки False → True (ніколи не знімаємо)
        if ac in nd_upgrade:
            cell_nd = ws.cell(row=row_idx, column=nd_col)
            if not cell_nd.value:
                cell_nd.value = True
                cell_nd.fill  = ORANGE_FILL

        touched.add(ac)

    count = len(touched)
    if count:
        print(
            f"   ✅ Оновлено set_codes у {count} існуючих attr_code "
            f"(нові рядки не створювались): {sorted(touched)}"
        )
    return count


# ─── Parallel fetch + streaming write ──────────────────────────────────────────────

def fetch_and_write_options_parallel(
    ac_agg: dict[str, "_AttrAgg"],
    nopt_agg: dict[str, "_AttrAgg"],
    ws,
    start_row: int,
) -> int:
    """
    Завантажує опції паралельно і одразу пише в ws, без накопичення в RAM.

    Приймає вже розбиті словники:
      • ac_agg   — select/multiselect attr_codes (нові, ще не в листі)
      • nopt_agg — числові/текстові attr_codes   (нові, ще не в листі)

    Архітектура:
      • as_completed → реальний паралелізм, не sequential blocking
      • write immediately after each future → RAM = тільки один attr за раз

    Повертає номер наступного вільного рядка.
    """
    total = len(ac_agg)

    print(
        f"\n⬇️  Опції: {total} унікальних select/multiselect attr_code ({OPTIONS_WORKERS} потоків)..."
        f"\n   Числові/текстові (без API): {len(nopt_agg)} attr_code"
    )

    current_row = start_row

    # ── Рядки-заглушки: нет API, пишемо одразу ─────────────────────────────
    for ac, agg in nopt_agg.items():
        _write_row_to_ws(ws, current_row, _row_vals(ac, agg, None))
        current_row += 1
    if nopt_agg:
        print(f"   ✅ Числові/текстові рядки ({len(nopt_agg)}): {list(nopt_agg)}")

    # ── Паралельне завантаження + immediate write ───────────────────────
    lock = Lock()
    done = [0]
    written = [0]

    def _worker(ac: str) -> tuple[str, list[dict], float]:
        t0 = time.monotonic()
        opts = _fetch_options_one_attr(ac, ac_agg[ac].set_codes)
        return ac, opts, time.monotonic() - t0

    fetch_start = time.monotonic()

    with ThreadPoolExecutor(max_workers=OPTIONS_WORKERS) as pool:
        future_map = {pool.submit(_worker, ac): ac for ac in ac_agg}

        # as_completed: обробляємо future одразу як воно готове.
        # brand (1250 стор.) не блокує інші 7 воркерів.
        for future in as_completed(future_map):
            ac = future_map[future]
            try:
                _, opts, elapsed = future.result()
                elapsed_s = f"{elapsed:.1f}s"
            except Exception as e:
                opts = []
                elapsed_s = "err"
                print(f"   ⚠️  ERROR attr={ac}: {e}")

            agg = ac_agg[ac]
            rows_for_attr = opts or [None]  # хоча б один рядок з порожнім option_code

            # openpyxl не thread-safe — пишемо під lock
            with lock:
                for opt in rows_for_attr:
                    _write_row_to_ws(ws, current_row, _row_vals(ac, agg, opt))
                    current_row += 1
                done[0] += 1
                written[0] += len(rows_for_attr)
                print(f"   [{done[0]}/{total}] attr={ac} → {len(opts)} опцій  ({elapsed_s})")

    fetch_elapsed = time.monotonic() - fetch_start
    print(
        f"   ⏱️  Завантаження та запис завершено за {fetch_elapsed:.1f}s\n"
        f"   Рядків записано: {written[0] + len(nopt_agg)}"
    )
    return current_row


def append_option_rows(
    pair_meta: dict[tuple[str, str], dict],
    existing_attr_codes: set[str],
) -> bool:
    """
    Відкриває xlsx, оновлює існуючі рядки і/або дописує нові, зберігає.

    Логіка розмежування:
      • attr_code вже є в листі → _update_existing_attr_set_codes:
            тільки дописуємо нові set_codes до існуючих рядків
      • attr_code новий         → fetch_and_write_options_parallel:
            завантажуємо опції з API і малюємо нові рядки

    Це усуває масове дублювання для загальних атрибутів
    (brand, weight, ratio тощо) при додаванні нових категорій.

    Повертає True якщо хоча б щось змінено.
    """
    import openpyxl as _xl
    wb = _xl.load_workbook(OUTPUT_PATH)
    ws, start_row = _setup_options_sheet(wb)

    # Будуємо агрегати один раз для всіх нових пар
    ac_agg, nopt_agg = _build_agg_dicts(pair_meta)
    all_agg: dict[str, _AttrAgg] = {**ac_agg, **nopt_agg}

    # Розбиваємо: існуючі attr_codes (in-place update) vs нові (нові рядки)
    update_agg   = {ac: agg for ac, agg in all_agg.items()  if ac in existing_attr_codes}
    new_ac_agg   = {ac: agg for ac, agg in ac_agg.items()   if ac not in existing_attr_codes}
    new_nopt_agg = {ac: agg for ac, agg in nopt_agg.items() if ac not in existing_attr_codes}

    print(
        f"\n📋 Розподіл attr_codes для нових set_codes:\n"
        f"   • Вже в листі — тільки set_codes дозаписуємо : {len(update_agg)}"
        + (f" {sorted(update_agg)}" if update_agg else "") + "\n"
        f"   • Нові select/multiselect (API + нові рядки)  : {len(new_ac_agg)}\n"
        f"   • Нові числові/текстові   (рядок-заглушка)   : {len(new_nopt_agg)}"
    )

    # In-place: дописуємо set_codes до існуючих рядків
    updated_count = _update_existing_attr_set_codes(ws, update_agg)

    # Нові рядки тільки для справді нових attr_codes
    final_row = fetch_and_write_options_parallel(new_ac_agg, new_nopt_agg, ws, start_row)
    rows_written = final_row - start_row

    if rows_written == 0 and updated_count == 0:
        wb.close()
        return False

    print(f"   💾 Збереження xlsx ({rows_written} нових рядків, {updated_count} attr_codes оновлено)...")
    save_start = time.monotonic()
    wb.save(OUTPUT_PATH)
    wb.close()
    print(f"   ✅ Збережено за {time.monotonic() - save_start:.1f}s")
    return True


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    main_start = time.monotonic()
    print("🚀 epicenter_export_attr_options.py — КРОК 5\n")

    if not OUTPUT_PATH.exists():
        print(
            "❌ epicenter_mappings.xlsx не знайдено.\n"
            "   Спочатку виконай КРОК 1: python scripts/epicenter_export_categories.py"
        )
        return

    mapped_set_codes = load_mapped_set_codes()
    if not mapped_set_codes:
        print(
            "\n⏭️  Немає заповнених epicenter_category_id у «Маппінг».\n"
            "   Виконай КРОК 2: python scripts/epicenter_map_categories.py"
        )
        return

    # Фільтруємо: тільки нові set_codes (яких ще немає в «Опції атрибутів»)
    existing_with_options = load_set_codes_with_options()
    new_set_codes = mapped_set_codes - existing_with_options

    if not new_set_codes:
        print("   ✅ Опції вже завантажено для всіх категорій.")
        print("   КРОК 6: Заповни prom_option_name (колонка H) у «Опції атрибутів».")
        return

    print(f"   Нових set_codes для завантаження опцій: {len(new_set_codes)} шт.")

    # Завантажуємо метадані атрибутів з «Сети атрибутів»
    pair_meta = load_attr_set_meta(new_set_codes)

    if not pair_meta:
        print(
            "\n⏭️  Немає атрибутів з isRequired=TRUE для нових категорій.\n"
            "   Можливі причини:\n"
            "   • Жоден обов'язковий атрибут не знайдено для цих set_codes\n"
            "   • Не заповнено isRequired у «Сети атрибутів»\n"
            "   Перевір лист «Сети атрибутів», колонку isRequired."
        )
        return

    # Завантажуємо attr_codes, які вже є в листі — для них тільки
    # дописуємо нові set_codes (без повторного рендеру тисяч рядків опцій)
    existing_attr_codes = load_existing_option_attr_codes()

    written = append_option_rows(pair_meta, existing_attr_codes)
    if not written:
        print("⚠️  Опцій не записано.")
        return

    total_elapsed = time.monotonic() - main_start
    print(
        f"\n✅ Оновлено: {OUTPUT_PATH}  (загальний час: {total_elapsed:.1f}s)\n"
        "   КРОК 6: Заповни prom_option_name (колонка H) у «Опції атрибутів» вручну або скриптом."
    )


if __name__ == "__main__":
    main()