"""
epicenter_export_attr_options.py
---------------------------------
КРОК 5 з 5 пайплайну маппінгу Prom → Epicenter.

Що робить:
  Завантажує з API опції атрибутів Epicenter і записує в лист «Опції атрибутів»
  ТІЛЬКИ для тих пар (set_code, attr_code), у яких заповнений prom_param_name
  у листі «Сети атрибутів» — і тільки для set_codes з «Маппінгу».

Передумова:
  • Лист «Маппінг» — заповнені epicenter_category_id.
  • Лист «Сети атрибутів» — заповнені prom_param_name для потрібних атрибутів.

Інкрементальна логіка:
  Якщо «Опції атрибутів» вже існує — дописує тільки нові set_codes,
  існуючі рядки не чіпає.

Наступний крок (КРОК 6):
  Заповни prom_option_name у «Опції атрибутів» (колонка H) вручну або скриптом.

Запуск:
    python scripts/epicenter_export_attr_options.py
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
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

ROOT        = Path(__file__).parents[1]
OUTPUT_PATH = ROOT / "data" / "markets" / "epicenter_mappings.xlsx"

OPTION_TYPES    = {"select", "multiselect"}
OPTIONS_WORKERS = 8
REQ_TIMEOUT     = (10, 30)
MAX_PAGES       = 200
FUTURE_TIMEOUT  = 90

# Атрибути з надто великою кількістю опцій — пропускаємо (не корисні для маппінгу).
# brand має 10 000+ опцій і множиться по всіх категоріях → Excel ліміт вичерпується.
SKIP_ATTRS: set[str] = {"brand", "country_of_origin"}

# Ключова зміна архітектури:
# Опції пишуться по УНІКАЛЬНОМУ attr_code, НЕ по парі (set_code, attr_code).
# set_codes і prom_params зберігаються через кому в окремих колонках.
# Це скорочує кількість рядків з ~1.3M до ~10K.
#
# needs_default у рядку = True якщо хоча б один set_code використовує цей attr
# без заповненого prom_param_name (червона клітинка в «Сети атрибутів»).


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
YELLOW_FILL  = PatternFill("solid", start_color="FFFF99", end_color="FFFF99")
GRAY_FILL    = PatternFill("solid", start_color="F2F2F2", end_color="F2F2F2")
BLUE_FILL    = PatternFill("solid", start_color="DEEAF1", end_color="DEEAF1")
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
    cell.border = THIN_BORDER
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
    Читає «Сети атрибутів» і повертає метадані для пар (set_code, attr_code)
    які потребують завантаження опцій. Включає пару якщо виконується хоча б одна умова:
      • prom_param_name заповнений  → треба опції для маппінгу Prom→Epicenter
      • isRequired = True           → треба опції щоб вибрати дефолт (червона клітинка)

    Обидва випадки — тільки для типів select / multiselect і тільки для set_codes з маппінгу.
    Поле `needs_default` в метаданих = True якщо prom_param_name порожній (червона клітинка).
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
        skipped_type = 0

        for row in rows[1:]:
            sc = _norm_id(row[idx["set_code"]])
            ac = _norm_id(row[idx["attr_code"]])
            if not sc or not ac or sc not in filter_set_codes:
                continue

            atype = str(row[idx["attr_type"]] or "").strip().lower()
            if atype not in OPTION_TYPES:
                skipped_type += 1
                continue

            prom = str(row[idx["prom_param_name"]] or "").strip() \
                if len(row) > idx["prom_param_name"] else ""
            is_required = str(row[idx["isRequired"]] or "").strip().upper() in ("TRUE", "1", "YES")

            # Включаємо пару якщо є prom_param_name АБО атрибут обов'язковий (червона клітинка)
            if not prom and not is_required:
                continue

            meta[(sc, ac)] = {
                "set_name":    str(row[idx["set_name_uk"]] or "").strip(),
                "attr_name":   str(row[idx["attr_name_uk"]] or "").strip(),
                "attr_type":   atype,
                "prom_param":  prom,
                "needs_default": not bool(prom),  # True = червона клітинка, треба вибрати дефолт
            }

        mapped_count  = sum(1 for m in meta.values() if not m["needs_default"])
        default_count = sum(1 for m in meta.values() if m["needs_default"])
        print(
            f"   select/multiselect пар для завантаження опцій: {len(meta)} шт.\n"
            f"   • з prom_param_name (маппінг):         {mapped_count}\n"
            f"   • без prom_param_name (дефолт, червоні): {default_count}\n"
            f"   • пропущено (не select/multiselect):   {skipped_type}"
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


# ─── API ──────────────────────────────────────────────────────────────────────

def _fetch_options_one_attr(attr_code: str, set_code: str) -> list[dict]:
    """Завантажує всі сторінки опцій для одного attr_code."""
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


def fetch_options_parallel(
    pair_meta: dict[tuple[str, str], dict],
) -> list[dict]:
    """
    Паралельно завантажує опції і повертає рядки для запису.

    Дедублікація по attr_code:
      Один attr_code може бути в N категоріях (сетах) — опції однакові.
      Пишемо кожний attr_code ОДИН РАЗ, зберігаємо всі set_codes через кому.
      Це скорочує ~1.3M рядків до ~10K.

    SKIP_ATTRS — атрибути з занадто багатьом опцій (не корисні для маппінгу).
    """
    # Агрегуємо дані по attr_code:
    #   first_set  — перший зустрінутий set_code (для API-запиту)
    #   set_codes  — всі set_codes що використовують цей attr
    #   prom_params — всі заповнені prom_param_name (unique)
    #   needs_default — True якщо хоча б один set_code не має prom_param_name
    class AttrAgg:
        __slots__ = ("first_set", "attr_name", "attr_type",
                     "set_codes", "prom_params", "needs_default")
        def __init__(self, sc: str, meta: dict) -> None:
            self.first_set    = sc
            self.attr_name    = meta["attr_name"]
            self.attr_type    = meta["attr_type"]
            self.set_codes:   list[str] = [sc]
            self.prom_params: list[str] = [meta["prom_param"]] if meta["prom_param"] else []
            self.needs_default = meta["needs_default"]

    ac_agg: dict[str, AttrAgg] = {}
    skipped_skip_attrs = 0

    for (sc, ac), meta in pair_meta.items():
        if ac in SKIP_ATTRS:
            skipped_skip_attrs += 1
            continue
        if ac not in ac_agg:
            ac_agg[ac] = AttrAgg(sc, meta)
        else:
            agg = ac_agg[ac]
            if sc not in agg.set_codes:
                agg.set_codes.append(sc)
            if meta["prom_param"] and meta["prom_param"] not in agg.prom_params:
                agg.prom_params.append(meta["prom_param"])
            if meta["needs_default"]:
                agg.needs_default = True

    total = len(ac_agg)
    if skipped_skip_attrs:
        print(f"   ⏭️  SKIP_ATTRS: пропущено {skipped_skip_attrs} пар для: {SKIP_ATTRS}")
    print(f"\n⬇️  Опції: {total} унікальних attr_code ({OPTIONS_WORKERS} потоків)...")

    options_cache: dict[str, list[dict]] = {}
    lock = Lock()
    done = [0]

    def _worker(ac: str) -> tuple[str, list[dict]]:
        return ac, _fetch_options_one_attr(ac, ac_agg[ac].first_set)

    with ThreadPoolExecutor(max_workers=OPTIONS_WORKERS) as pool:
        pending = {pool.submit(_worker, ac): ac for ac in ac_agg}
        for f, ac in pending.items():
            try:
                _, opts = f.result(timeout=FUTURE_TIMEOUT)
                options_cache[ac] = opts
            except TimeoutError:
                options_cache[ac] = []
                print(f"   ⏱️  TIMEOUT attr={ac} (>{FUTURE_TIMEOUT}s)")
            except Exception as e:
                options_cache[ac] = []
                print(f"   ⚠️  ERROR attr={ac}: {e}")
            with lock:
                done[0] += 1
                print(f"   [{done[0]}/{total}] attr={ac} → {len(options_cache[ac])} опцій")

    rows: list[dict] = []
    for ac, agg in ac_agg.items():
        opts = options_cache.get(ac, [])
        base = {
            "attr_code":     ac,
            "attr_name_uk":  agg.attr_name,
            "attr_type":     agg.attr_type,
            "set_codes":     ", ".join(agg.set_codes),
            "prom_params":   ", ".join(agg.prom_params),
            "needs_default": agg.needs_default,
        }
        if not opts:
            rows.append({**base, "option_code": "", "option_name_uk": "", "prom_option_name": ""})
        else:
            for opt in opts:
                rows.append({
                    **base,
                    "option_code":    opt.get("code", ""),
                    "option_name_uk": _parse_option_name(opt),
                    "prom_option_name": "",
                })

    mapped_rows  = sum(1 for r in rows if not r["needs_default"])
    default_rows = sum(1 for r in rows if r["needs_default"])
    print(
        f"✅ Зібрано {len(rows)} рядків (дедубліковано по attr_code):\n"
        f"   • маппінг (prom_param_name заповнений): {mapped_rows}\n"
        f"   • дефолт  (червоні клітинки):           {default_rows}"
    )
    return rows


# ─── Writers ──────────────────────────────────────────────────────────────────

# Нова схема: колонка set_code видалена, замінена на set_codes (через кому) і prom_params.
HEADERS_OPTIONS    = ["attr_code", "attr_name_uk", "attr_type",
                      "option_code", "option_name_uk", "prom_option_name",
                      "needs_default", "set_codes", "prom_params"]
COL_WIDTHS_OPTIONS = [30, 42, 16, 30, 45, 45, 14, 60, 60]


def _write_option_rows(ws, option_rows: list[dict], start_row: int) -> None:
    for ri, row in enumerate(option_rows, start_row):
        needs_default = row.get("needs_default", False)
        vals = [
            row["attr_code"],
            row["attr_name_uk"],
            row["attr_type"],
            row["option_code"],
            row["option_name_uk"],
            row.get("prom_option_name", ""),
            needs_default,
            row.get("set_codes", ""),
            row.get("prom_params", ""),
        ]
        for ci, val in enumerate(vals, 1):
            if needs_default:
                fill = ORANGE_FILL
            else:
                fill = (BLUE_FILL    if ci in (4, 5)    # option_code, option_name_uk
                        else YELLOW_FILL if ci == 6      # prom_option_name
                        else GRAY_FILL   if ci >= 8      # set_codes, prom_params
                        else None)
            _data(ws.cell(row=ri, column=ci, value=val), fill=fill)


def build_options_sheet(wb: Workbook, option_rows: list[dict]) -> None:
    ws = wb.create_sheet("Опції атрибутів")
    for ci, (h, w) in enumerate(zip(HEADERS_OPTIONS, COL_WIDTHS_OPTIONS), 1):
        _hdr(ws.cell(row=1, column=ci, value=h))
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.cell(
        row=1, column=8,
        value="🟠 помаранч. = needs_default (вибери дефолт) | 🟡 F = prom_option_name (заповни відповідник) | ✂️ SKIP_ATTRS: brand, country_of_origin",
    ).font = Font(bold=True, color="7F6000", name="Arial", size=9)
    _write_option_rows(ws, option_rows, start_row=2)
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{get_column_letter(len(HEADERS_OPTIONS))}{len(option_rows) + 1}"


def append_option_rows(option_rows: list[dict]) -> None:
    """Дописує рядки в існуючий лист «Опції атрибутів»."""
    import openpyxl as _xl
    wb = _xl.load_workbook(OUTPUT_PATH)
    if "Опції атрибутів" not in wb.sheetnames:
        build_options_sheet(wb, option_rows)
    else:
        ws = wb["Опції атрибутів"]
        _write_option_rows(ws, option_rows, start_row=ws.max_row + 1)
    wb.save(OUTPUT_PATH)
    wb.close()
    print(f"   ✅ Записано {len(option_rows)} рядків у «Опції атрибутів».")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
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
            "\n⏭️  Немає атрибутів типу select/multiselect для завантаження опцій.\n"
            "   Можливі причини:\n"
            "   • Жоден атрибут не є select/multiselect для цих категорій\n"
            "   • Не заповнено prom_param_name І немає isRequired=True у «Сети атрибутів»\n"
            "   Якщо є незаповнені prom_param_name — виконай КРОК 4:\n"
            "   • Автоматично: python scripts/epicenter_map_attributes.py\n"
            "   • Вручну:      колонка J у «Сети атрибутів»"
        )
        return

    option_rows = fetch_options_parallel(pair_meta)

    if not option_rows:
        print("⚠️  Опцій не отримано.")
        return

    append_option_rows(option_rows)

    print(
        f"\n✅ Оновлено: {OUTPUT_PATH}\n"
        "   КРОК 6: Заповни prom_option_name (колонка H) у «Опції атрибутів» вручну або скриптом."
    )


if __name__ == "__main__":
    main()
