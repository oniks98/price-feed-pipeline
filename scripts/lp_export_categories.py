"""
lp_export_categories.py
═══════════════════════
Завантажує дерево категорій LP через API і записує листові категорії
в data/lp/lp_category.csv для подальшого ручного маппінгу на Prom.

Структура CSV (як secur_category.csv):
  • Рядки 1-2  — завжди Уцінка (site + prom, Номер_групи=delete)
  • Далі       — кожна листова категорія × 2 рядки (site + prom)

Заповнюється автоматично:
    №, channel, Номер_групи (тільки для Уцінки), category id,
    Назва у постачальника (повний шлях: Батько > Дитина > Листова)

Порожньо (заповнити вручну в Excel):
    coef, threshold, Номер_групи, Назва_групи,
    Ідентифікатор_підрозділу, Посилання_підрозділу, ...

MERGE-РЕЖИМ (повторний запуск):
    Існуючі рядки (з ручним маппінгом) — НЕ чіпаються.
    Додаються тільки нові category id яких ще немає в CSV (× 2 рядки).

data/lp/lp_category_tree.txt — довідковий файл з повною ієрархією,
    перезаписується щоразу.

Запуск:
    python scripts/lp_export_categories.py
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path
from typing import Iterator

import requests
from dotenv import load_dotenv

# ─────────────────────────────────────────────────────────────────────
# Шляхи
# ─────────────────────────────────────────────────────────────────────
_PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
_ENV_FILE     = _PROJECT_ROOT / "suppliers" / ".env"
_OUTPUT_CSV   = _PROJECT_ROOT / "data" / "lp" / "lp_category.csv"
_OUTPUT_TREE  = _PROJECT_ROOT / "data" / "lp" / "lp_category_tree.txt"

# ─────────────────────────────────────────────────────────────────────
# Константи
# ─────────────────────────────────────────────────────────────────────
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
    "Назва_Характеристики",
    "Одиниця_виміру_Характеристики",
    "Значення_Характеристики",
    "feed",
    "category id",
    "Назва у постачальника",
]

# ─────────────────────────────────────────────────────────────────────
# API SESSION
# ─────────────────────────────────────────────────────────────────────

def build_session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "X-Api-Key": token,
        "Accept":    "application/json",
    })
    return s


def fetch_category_tree(session: requests.Session, base_url: str) -> list[dict]:
    url = f"{base_url.rstrip('/')}/external/catalog/category/list/tree"
    print(f"📡 GET {url}")
    resp = session.get(url, timeout=30)

    if resp.status_code == 401:
        sys.exit("❌ HTTP 401 — невірний LP_API_TOKEN")

    resp.raise_for_status()
    payload: dict = resp.json()

    if not payload.get("status"):
        sys.exit(f"❌ API повернув статус=false: {payload}")

    data = payload.get("data")
    if not isinstance(data, list):
        sys.exit(f"❌ Unexpected API response shape: {payload}")

    return data


# ─────────────────────────────────────────────────────────────────────
# LEAF EXTRACTION
# ─────────────────────────────────────────────────────────────────────

def _name_uk(node: dict) -> str:
    """Повертає name.uk вузла, fallback → name.ru."""
    return (
        node.get("name", {}).get("uk", "")
        or node.get("name", {}).get("ru", "")
    ).strip()


def iter_leaves(
    nodes: list[dict],
    path: list[str] | None = None,
) -> Iterator[tuple[dict, list[str]]]:
    """
    Рекурсивно обходить дерево.
    Yield: (leaf_node, full_path) — тільки для листових вузлів.
    full_path містить назви всіх рівнів від кореня до листа.
    Листовий = children відсутній або порожній.
    """
    if path is None:
        path = []

    for node in nodes:
        current_path = path + [_name_uk(node)]
        children: list[dict] = node.get("children") or []

        if children:
            yield from iter_leaves(children, current_path)
        else:
            yield node, current_path


# ─────────────────────────────────────────────────────────────────────
# CSV ROW BUILDER
# ─────────────────────────────────────────────────────────────────────

def _make_row(
    idx: int,
    channel: str,
    category_code: str,
    full_path: str,
    delete_marker: bool = False,
) -> list[str]:
    """
    Будує один CSV-рядок.
    delete_marker=True → Номер_групи="delete" (для категорії Уцінка).
    """
    row: dict[str, str] = {col: "" for col in CSV_HEADER}
    row["№"]                     = str(idx)
    row["channel"]               = channel
    row["Номер_групи"]           = "delete" if delete_marker else ""
    row["category id"]           = category_code
    row["Назва у постачальника"] = full_path
    return [row[col] for col in CSV_HEADER]


# ─────────────────────────────────────────────────────────────────────
# CSV — MERGE MODE
# ─────────────────────────────────────────────────────────────────────

def load_existing_codes(path: Path) -> tuple[set[str], int]:
    """
    Читає існуючий CSV → (set існуючих category id, кількість рядків з даними).
    Рядки з порожнім category id ігноруються при підрахунку кодів,
    але враховуються в row_count для правильної нумерації нових рядків.
    """
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
    leaves: list[tuple[dict, list[str]]],
    output_path: Path,
) -> tuple[int, int]:
    """
    Merge-режим: додає тільки нові категорії (яких ще немає в CSV).
    Кожна категорія → 2 рядки: site + prom.
    Уцінка (UTSINKA_CODE) — перші два рядки, Номер_групи=delete.
    Назва у постачальника = повний шлях від кореня (Батько > ... > Лист).

    Повертає (кількість_нових_категорій, кількість_вже_існуючих_категорій).
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    existing_codes, existing_row_count = load_existing_codes(output_path)

    # Складаємо список нових категорій для запису
    # Уцінка — завжди першою (якщо ще немає в CSV)
    to_write: list[tuple[str, str, bool]] = []  # (code, full_path, is_delete)

    if UTSINKA_CODE not in existing_codes:
        to_write.append((UTSINKA_CODE, UTSINKA_NAME, True))

    for leaf, path_ in leaves:
        code = str(leaf.get("code", "")).strip()
        if not code or code == UTSINKA_CODE:
            continue
        if code in existing_codes:
            continue
        full_path = " > ".join(path_)
        to_write.append((code, full_path, False))

    if not to_write:
        return 0, len(existing_codes)

    first_run = not output_path.exists() or output_path.stat().st_size == 0
    current_idx = existing_row_count + 1

    with open(output_path, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")

        if first_run:
            writer.writerow(CSV_HEADER)

        for code, full_path, is_delete in to_write:
            for channel in CHANNELS:
                writer.writerow(_make_row(current_idx, channel, code, full_path, is_delete))
                current_idx += 1

    return len(to_write), len(existing_codes)


# ─────────────────────────────────────────────────────────────────────
# TREE (довідковий файл — завжди перезаписується)
# ─────────────────────────────────────────────────────────────────────

def write_tree(
    leaves: list[tuple[dict, list[str]]],
    output_path: Path,
) -> None:
    """
    Записує повну ієрархію листових категорій у txt.
    Перезаписується при кожному запуску.
    Формат: №   [code]   Батько > ... > Лист   (N тов.)
    """
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("LP — листові категорії (для маппінгу на Prom)\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"{'№':>5}  {'Код':<8}  {'Шлях'}\n")
        f.write("-" * 70 + "\n")

        for idx, (leaf, path) in enumerate(leaves, start=1):
            code      = str(leaf.get("code", "")).strip()
            products  = leaf.get("productsCount", 0)
            breadcrumb = " > ".join(path)
            f.write(f"{idx:>5}  {code:<8}  {breadcrumb}  ({products} тов.)\n")


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
        sys.exit(
            "❌ LP_API_BASE_URL не знайдено в suppliers/.env\n"
            "   Додайте рядок: LP_API_BASE_URL=https://api.b2b.logicpower.ua"
        )

    print(f"🔑 Token: {token[:8]}***")
    print(f"🌐 Base URL: {base_url}\n")

    session = build_session(token)

    tree = fetch_category_tree(session, base_url)
    print(f"✅ Кореневих категорій: {len(tree)}")

    leaves = list(iter_leaves(tree))
    print(f"🍃 Листових категорій в API: {len(leaves)}")

    added, existing = write_csv(leaves, _OUTPUT_CSV)

    if added == 0:
        print(f"✅ CSV — нових категорій немає ({existing} вже є у файлі)")
    else:
        total_rows = (existing + added) * 2
        print(
            f"✅ CSV — додано {added} нових категорій × 2 рядки = {added * 2} рядків\n"
            f"   Було: {existing} категорій  →  Стало: {existing + added}  "
            f"({total_rows} рядків разом)"
        )
    print(f"   → {_OUTPUT_CSV}")

    write_tree(leaves, _OUTPUT_TREE)
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
