"""
Зіставляє категорії фіду (kasta_coefficients.csv) з довідником Kasta (categories_kasta.csv).
Результат зберігається у data/markets/mapping.csv для ручної перевірки та корекції.

Запуск:
    python scripts/kasta_map_categories.py

Алгоритм:
    1. Для кожного рядка kasta_coefficients.csv беремо category_name (напр. "Камеры видеонаблюдения")
    2. Нормалізуємо текст (нижній регістр, trim)
    3. Шукаємо найближчий збіг серед kasta_item (Вид*:21) у довіднику Kasta
    4. Використовуємо difflib fuzzy matching по повному шляху для точності
    5. Записуємо результат з колонкою score для контролю якості
"""

import csv
import difflib
import re
from pathlib import Path

# ── Шляхи ────────────────────────────────────────────────────────────────────

BASE = Path(__file__).parents[1] / "data" / "markets"
COEFFICIENTS_FILE = BASE / "kasta_coefficients.csv"
KASTA_DICT_FILE   = BASE / "categories_kasta.csv"
OUTPUT_FILE       = BASE / "mapping.csv"

# Поріг схожості (0.0–1.0). Нижче — відмічаємо як "потребує перевірки"
MATCH_THRESHOLD = 0.40


# ── Утиліти ───────────────────────────────────────────────────────────────────

def normalize(text: str) -> str:
    """Нижній регістр, прибираємо зайві символи для чистого порівняння."""
    text = text.lower().strip()
    # Прибираємо спецсимволи, залишаємо букви, цифри, пробіли
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_child(category_name: str) -> str:
    """
    З 'Батьківська > Дочірня' повертає 'Дочірня'.
    Якщо роздільника немає — повертає повну назву.
    """
    parts = category_name.split(">")
    return parts[-1].strip() if len(parts) > 1 else category_name.strip()


# ── Завантаження даних ────────────────────────────────────────────────────────

def load_coefficients() -> list[dict]:
    """Завантажує category_id, category_name, coefficient з kasta_coefficients.csv."""
    rows = []
    with open(COEFFICIENTS_FILE, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            rows.append({
                "category_id":   row["category_id"].strip(),
                "category_name": row["category_name"].strip(),
                "coefficient":   row["coefficient"].strip(),
            })
    print(f"✅ Завантажено {len(rows)} категорій фіду")
    return rows


def load_kasta_dict() -> list[dict]:
    """
    Завантажує довідник Kasta.
    Структура: Приналежність > Група > Підгрупа > Вид
    """
    entries = []
    with open(KASTA_DICT_FILE, encoding="utf-8-sig") as f:
        reader = csv.reader(f, delimiter=";")
        next(reader, None)  # пропускаємо заголовок
        for row in reader:
            if len(row) < 4:
                continue
            l1 = row[0].strip()
            l2 = row[1].strip()
            l3 = row[2].strip()
            l4 = row[3].strip()
            if not l4:
                continue
            entries.append({
                "l1": l1, "l2": l2, "l3": l3, "l4": l4,
                # Нормалізовані варіанти для порівняння
                "norm_l4":   normalize(l4),
                "norm_l3l4": normalize(f"{l3} {l4}"),
                "norm_full": normalize(f"{l1} {l2} {l3} {l4}"),
            })
    print(f"✅ Завантажено {len(entries)} позицій довідника Kasta")
    return entries


# ── Матчинг ───────────────────────────────────────────────────────────────────

def find_best_match(category_name: str, kasta_entries: list[dict]) -> tuple[dict | None, float]:
    """
    Шукає найкращий збіг для category_name серед довідника Kasta.

    Стратегія (від точного до нечіткого):
    1. Точний збіг по child-частині назви (після ">")
    2. Fuzzy по child-частині (l4)
    3. Fuzzy по l3+l4 (підгрупа + вид)
    4. Fuzzy по повному шляху фіду vs повний шлях Kasta

    Повертає (best_entry | None, score 0.0–1.0)
    """
    child = extract_child(category_name)
    norm_child = normalize(child)
    norm_full  = normalize(category_name.replace(">", " "))

    best_entry: dict | None = None
    best_score: float = 0.0

    norm_l4_list   = [e["norm_l4"]   for e in kasta_entries]
    norm_l3l4_list = [e["norm_l3l4"] for e in kasta_entries]
    norm_full_list = [e["norm_full"]  for e in kasta_entries]

    def _update(matches: list[str], source_list: list[str], boost: float = 1.0):
        nonlocal best_entry, best_score
        if not matches:
            return
        matched_norm = matches[0]
        idx = source_list.index(matched_norm)
        score = difflib.SequenceMatcher(None, norm_child, matched_norm).ratio() * boost
        if score > best_score:
            best_score = score
            best_entry = kasta_entries[idx]

    # 1. Точний збіг по l4
    exact = [e for e in kasta_entries if e["norm_l4"] == norm_child]
    if exact:
        return exact[0], 1.0

    # 2. Fuzzy по l4
    m = difflib.get_close_matches(norm_child, norm_l4_list, n=1, cutoff=0.3)
    _update(m, norm_l4_list, boost=1.0)

    # 3. Fuzzy по l3+l4 (більш специфічне, підвищуємо вагу)
    m = difflib.get_close_matches(norm_child, norm_l3l4_list, n=1, cutoff=0.3)
    _update(m, norm_l3l4_list, boost=0.95)

    # 4. Fuzzy по повному шляху фіду vs повний шлях Kasta
    m = difflib.get_close_matches(norm_full, norm_full_list, n=1, cutoff=0.3)
    if m:
        idx = norm_full_list.index(m[0])
        score = difflib.SequenceMatcher(None, norm_full, m[0]).ratio() * 0.85
        if score > best_score:
            best_score = score
            best_entry = kasta_entries[idx]

    return best_entry, round(best_score, 3)


# ── Головна функція ───────────────────────────────────────────────────────────

def main() -> None:
    coefficients = load_coefficients()
    kasta_entries = load_kasta_dict()

    results = []
    unmatched = []

    for item in coefficients:
        entry, score = find_best_match(item["category_name"], kasta_entries)

        if entry and score >= MATCH_THRESHOLD:
            status = "OK" if score >= 0.75 else "ПЕРЕВІРИТИ"
            results.append({
                "ID категорії фіду":  item["category_id"],
                "Категорії фіду":     item["category_name"],
                "Приналежність*:6":   entry["l1"],
                "Група*:13":          entry["l2"],
                "Підгрупа*:14":       entry["l3"],
                "Вид*:21":            entry["l4"],
                "score":              score,
                "статус":             status,
            })
        else:
            unmatched.append(item["category_name"])
            results.append({
                "ID категорії фіду":  item["category_id"],
                "Категорії фіду":     item["category_name"],
                "Приналежність*:6":   "",
                "Група*:13":          "",
                "Підгрупа*:14":       "",
                "Вид*:21":            "",
                "score":              score,
                "статус":             "НЕ ЗНАЙДЕНО",
            })

    # Сортуємо: спочатку НЕ ЗНАЙДЕНО і ПЕРЕВІРИТИ — щоб зручно доповнювати вручну
    order = {"НЕ ЗНАЙДЕНО": 0, "ПЕРЕВІРИТИ": 1, "OK": 2}
    results.sort(key=lambda x: order.get(x["статус"], 3))

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "ID категорії фіду", "Категорії фіду",
            "Приналежність*:6", "Група*:13", "Підгрупа*:14", "Вид*:21",
            "score", "статус",
        ], delimiter=";")
        writer.writeheader()
        writer.writerows(results)

    ok_count      = sum(1 for r in results if r["статус"] == "OK")
    check_count   = sum(1 for r in results if r["статус"] == "ПЕРЕВІРИТИ")
    missing_count = len(unmatched)

    print(f"\n📊 Результат:")
    print(f"   ✅ OK           : {ok_count}")
    print(f"   ⚠️  ПЕРЕВІРИТИ  : {check_count}")
    print(f"   ❌ НЕ ЗНАЙДЕНО : {missing_count}")
    print(f"\n💾 Збережено → {OUTPUT_FILE}")

    if unmatched:
        print(f"\n❌ Без збігу ({len(unmatched)}):")
        for name in unmatched:
            print(f"   - {name}")


if __name__ == "__main__":
    main()
