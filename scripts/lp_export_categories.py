"""
lp_export_categories.py
═══════════════════════════════════════════════════════════════════════
Завантажує категорії LP через API товарів — тільки ті категорії,
в яких є товари виробників з TARGET_MANUFACTURERS.

Логіка:
  1. GET /external/catalog/product/list/all  (всі сторінки)
  2. Для кожного товару: перевіряємо manufacturer.name ∈ TARGET_MANUFACTURERS
  3. Збираємо унікальні categories[].code → будуємо дерево категорій
  4. Записуємо в lp_category.csv (merge-режим, як раніше)

Структура CSV (як secur_category.csv):
  • Рядки 1-2  — завжди Уцінка (site + prom, Номер_групи=delete)
  • Далі       — кожна категорія × 2 рядки (site + prom)

MERGE-РЕЖИМ (повторний запуск):
  Існуючі рядки (з ручним маппінгом) — НЕ чіпаються.
  Додаються тільки нові category id яких ще немає в CSV.

data/lp/lp_category_tree.txt — довідковий файл, перезаписується щоразу.

Запуск:
    python scripts/lp_export_categories.py
"""

from __future__ import annotations

import csv
import math
import os
import sys
from pathlib import Path
from typing import Iterator

import requests
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────
# Конфіг
# ─────────────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
_ENV_FILE     = _PROJECT_ROOT / "suppliers" / ".env"
_OUTPUT_CSV   = _PROJECT_ROOT / "data" / "lp" / "lp_category.csv"
_OUTPUT_TREE  = _PROJECT_ROOT / "data" / "lp" / "lp_category_tree.txt"

PAGE_SIZE = 500

# Виробники, для яких збираємо категорії (case-insensitive порівняння)
TARGET_MANUFACTURERS: frozenset[str] = frozenset({
    "logicpower",
    "greenvision",
})

UTSINKA_CODE = "12261"
UTSINKA_NAME = "Уцінка"

CHANNELS = ("site", "prom")

CSV_HEADER: list[str] = [
    "№",
    "Линк категории поставщика",
    "channel",
    "prefix",
    "coef",
    "threshold",
    "Номер_групи",
    "Назва_групи",
    "Ідентифікатор_підрозділу",
    "Посилання_підрозділу",
    "Особисті_нотатки",
    "Ярлик",
    "feed",
    "category id",
    "Назва у постачальника",
    "Назва_Характеристики",
    "Одиниця_виміру_Характеристики",
    "Значення_Характеристики",
]


# ─────────────────────────────────────────────────────────────────────
# API SESSION
# ─────────────────────────────────────────────────────────────────────

def build_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "X-Api-Key":       token,
        "Accept":          "application/json",
        # Явно вимикаємо brotli — сервер повертає 'br', але brotlicffi
        # падає на великих chunked-відповідях. Gzip стабільніший.
        "Accept-Encoding": "gzip, deflate",
    })
    return s


def _check_payload(payload: dict, label: str) -> None:
    """Кидає SystemExit якщо API повернув статус=false."""
    if not payload.get("status"):
        sys.exit(f"❌ API status=false [{label}]: {payload}")


def fetch_page(
    session: requests.Session,
    base_url: str,
    page: int,
) -> dict:
    url = (
        f"{base_url.rstrip('/')}/external/catalog/product/list/all"
        f"?pageSize={PAGE_SIZE}&pageNum={page}"
    )
    resp = session.get(url, timeout=60)

    if resp.status_code == 401:
        sys.exit("❌ HTTP 401 — невірний LP_API_TOKEN")

    resp.raise_for_status()
    payload: dict = resp.json()
    _check_payload(payload, f"products page={page}")
    return payload.get("data", {})


def fetch_category_tree(
    session: requests.Session,
    base_url: str,
) -> dict[str, str]:
    """
    GET /external/catalog/category/list/tree
    Повертає dict[code → 'Батько > ... > Листок'] для всіх категорій.
    """
    url = f"{base_url.rstrip('/')}/external/catalog/category/list/tree"
    resp = session.get(url, timeout=60)

    if resp.status_code == 401:
        sys.exit("❌ HTTP 401 — невірний LP_API_TOKEN")

    resp.raise_for_status()
    payload: dict = resp.json()
    _check_payload(payload, "category tree")

    code_to_path: dict[str, str] = {}

    def _walk(nodes: list[dict], parent_path: str) -> None:
        for node in nodes:
            code = str(node.get("code", "")).strip()
            name_obj = node.get("name") or {}
            if isinstance(name_obj, dict):
                name = (name_obj.get("uk") or name_obj.get("ru") or "").strip()
            else:
                name = str(name_obj).strip()
            if not code or not name:
                continue
            full_path = f"{parent_path} > {name}" if parent_path else name
            code_to_path[code] = full_path
            _walk(node.get("children") or [], full_path)

    _walk(payload.get("data", []), "")
    return code_to_path


# ─────────────────────────────────────────────────────────────────────
# PRODUCT → CATEGORY COLLECTOR
# ─────────────────────────────────────────────────────────────────────

def _manufacturer_name(product: dict) -> str:
    """Повертає ім'я виробника нижнім регістром, або ''."""
    return (
        (product.get("manufacturer") or {}).get("name") or ""
    ).strip().lower()


def _is_target(product: dict) -> bool:
    """True якщо виробник входить до TARGET_MANUFACTURERS."""
    return _manufacturer_name(product) in TARGET_MANUFACTURERS


def _collect_categories(
    product: dict,
    category_paths: dict[str, str],
) -> Iterator[tuple[str, str]]:
    """
    Yield (code, breadcrumb) з product.categories.
    Спочатку шукає повний шлях у category_paths (з дерева).
    Фолбек: збирає назви з product.categories[].name.
    """
    cats: list[dict] = product.get("categories") or []
    if not cats:
        return

    leaf = cats[-1]
    code = str(leaf.get("code", "")).strip()
    if not code:
        return

    if code in category_paths:
        yield code, category_paths[code]
        return

    # Fallback: збираємо назви з масиву categories товару
    names: list[str] = []
    for cat in cats:
        name_obj = cat.get("name") or {}
        if isinstance(name_obj, dict):
            name = (name_obj.get("uk") or name_obj.get("ru") or "").strip()
        else:
            name = str(name_obj).strip()
        if name:
            names.append(name)

    breadcrumb = " > ".join(names) if names else f"[code:{code}]"
    yield code, breadcrumb


def iter_target_categories(
    session: requests.Session,
    base_url: str,
    category_paths: dict[str, str],
) -> Iterator[tuple[str, str, str]]:
    """
    Пагінує всі сторінки, фільтрує по TARGET_MANUFACTURERS.
    Yield: (category_code, breadcrumb, manufacturer_name)
    Дедуплікація — зовні.
    """
    print(f"📡 GET page=1 ...")
    data = fetch_page(session, base_url, page=1)

    items = data.get("items", [])
    total = data.get("totalItems", 0)
    total_pages = math.ceil(total / PAGE_SIZE) if total else 1

    print(f"📦 Товарів загалом: {total}  |  сторінок: {total_pages}")

    def process_page_items(page_items: list[dict]) -> Iterator[tuple[str, str, str]]:
        for product in page_items:
            if not _is_target(product):
                continue
            mfr = _manufacturer_name(product)
            for code, breadcrumb in _collect_categories(product, category_paths):
                yield code, breadcrumb, mfr

    yield from process_page_items(items)

    for page in range(2, total_pages + 1):
        print(f"📡 GET page={page}/{total_pages} ...")
        data = fetch_page(session, base_url, page=page)
        yield from process_page_items(data.get("items", []))


def collect_unique_categories(
    session: requests.Session,
    base_url: str,
) -> dict[str, dict]:
    """
    Повертає dict[code → {breadcrumb, manufacturers: set[str]}].
    Якщо один code зустрівся у двох виробників — об'єднуємо.
    """
    print("🌲 Завантаження дерева категорій ...")
    category_paths = fetch_category_tree(session, base_url)
    print(f"   → {len(category_paths)} категорій у дереві\n")

    result: dict[str, dict] = {}

    for code, breadcrumb, mfr in iter_target_categories(session, base_url, category_paths):
        if code not in result:
            result[code] = {
                "breadcrumb":    breadcrumb,
                "manufacturers": {mfr},
            }
        else:
            result[code]["manufacturers"].add(mfr)

    return result


# ─────────────────────────────────────────────────────────────────────
# CSV
# ─────────────────────────────────────────────────────────────────────

def _make_row(
    idx: int,
    channel: str,
    category_code: str,
    full_path: str,
    delete_marker: bool = False,
) -> list[str]:
    row: dict[str, str] = {col: "" for col in CSV_HEADER}
    row["№"]                     = str(idx)
    row["channel"]               = channel
    row["Номер_групи"]           = "delete" if delete_marker else ""
    row["category id"]           = category_code
    row["Назва у постачальника"] = full_path
    return [row[col] for col in CSV_HEADER]


def load_existing_codes(path: Path) -> tuple[set[str], int]:
    """(set<code>, row_count) — для merge-режиму."""
    if not path.exists() or path.stat().st_size == 0:
        return set(), 0

    codes: set[str] = set()
    row_count = 0

    with open(path, encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f, delimiter=";"):
            code = row.get("category id", "").strip()
            if code:
                codes.add(code)
            row_count += 1

    return codes, row_count


def write_csv(
    categories: dict[str, dict],
    output_path: Path,
) -> tuple[int, int]:
    """
    Merge-режим: додає тільки нові категорії.
    Уцінка — перші 2 рядки (якщо ще немає).
    Повертає (додано, вже_існувало).
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    existing_codes, existing_row_count = load_existing_codes(output_path)

    to_write: list[tuple[str, str, bool]] = []  # (code, breadcrumb, is_delete)

    if UTSINKA_CODE not in existing_codes:
        to_write.append((UTSINKA_CODE, UTSINKA_NAME, True))

    for code, meta in categories.items():
        if code == UTSINKA_CODE or code in existing_codes:
            continue
        to_write.append((code, meta["breadcrumb"], False))

    if not to_write:
        return 0, len(existing_codes)

    first_run = not output_path.exists() or output_path.stat().st_size == 0
    current_idx = existing_row_count + 1

    with open(output_path, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")

        if first_run:
            writer.writerow(CSV_HEADER)

        for code, breadcrumb, is_delete in to_write:
            for channel in CHANNELS:
                writer.writerow(_make_row(current_idx, channel, code, breadcrumb, is_delete))
                current_idx += 1

    return len(to_write), len(existing_codes)


# ─────────────────────────────────────────────────────────────────────
# TREE (довідковий файл)
# ─────────────────────────────────────────────────────────────────────

def write_tree(
    categories: dict[str, dict],
    output_path: Path,
) -> None:
    """Перезаписує довідковий txt з усіма знайденими категоріями."""
    mfr_label = ", ".join(sorted(m.capitalize() for m in TARGET_MANUFACTURERS))

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"LP — категорії для виробників: {mfr_label}\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"{'№':>5}  {'Код':<10}  {'Виробники':<25}  Шлях\n")
        f.write("-" * 70 + "\n")

        for idx, (code, meta) in enumerate(categories.items(), start=1):
            mfrs = ", ".join(sorted(m.capitalize() for m in meta["manufacturers"]))
            f.write(f"{idx:>5}  {code:<10}  {mfrs:<25}  {meta['breadcrumb']}\n")


# ─────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────

def main() -> None:
    load_dotenv(_ENV_FILE)

    token    = os.getenv("LP_API_TOKEN", "").strip()
    base_url = os.getenv("LP_API_BASE_URL", "").strip()

    if not token:
        sys.exit("❌ LP_API_TOKEN не знайдено в suppliers/.env")
    if not base_url:
        sys.exit("❌ LP_API_BASE_URL не знайдено в suppliers/.env")

    print(f"🔑 Token: {token[:8]}***")
    print(f"🌐 Base URL: {base_url}")
    mfr_label = " + ".join(sorted(m.capitalize() for m in TARGET_MANUFACTURERS))
    print(f"🏭 Виробники: {mfr_label}\n")

    session = build_session(token)

    categories = collect_unique_categories(session, base_url)

    if not categories:
        print(f"⚠️ Жодної категорії не знайдено для виробників: {mfr_label}")
        return

    print(f"\n✅ Унікальних категорій знайдено: {len(categories)}")

    added, existing = write_csv(categories, _OUTPUT_CSV)

    if added == 0:
        print(f"✅ CSV — нових категорій немає ({existing} вже є у файлі)")
    else:
        total = existing + added
        print(
            f"✅ CSV — додано {added} нових категорій × 2 рядки = {added * 2} рядків\n"
            f"   Було: {existing}  →  Стало: {total}  ({total * 2} рядків разом)"
        )
    print(f"   → {_OUTPUT_CSV}")

    write_tree(categories, _OUTPUT_TREE)
    print(f"🌲 Tree → {_OUTPUT_TREE}  (оновлено)\n")

    if added > 0:
        print("─" * 55)
        print("📋 Нові категорії потрібно заповнити в Excel lp_category.csv:")
        print("   Кожна категорія = 2 рядки (site + prom)")
        print("   Обов'язкові: coef · threshold · Ідентифікатор_підрозділу")
        print("                Посилання_підрозділу · Номер_групи · Назва_групи")
        print("   Дивіться lp_category_tree.txt для орієнтування в ієрархії")


if __name__ == "__main__":
    main()
