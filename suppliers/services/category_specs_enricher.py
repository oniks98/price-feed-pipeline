"""
Сервіс для додавання характеристик на основі категорії постачальника.

Підтримує два режими пошуку:
  - За URL категорії постачальника (retail.py — колонка "Линк категории поставщика")
  - За category id           (feed.py  — колонка "category id")

ПРИКЛАД CSV СТРУКТУРИ:
Линк категории поставщика;...;Назва_Характеристики;Одиниця_виміру_Характеристики;Значення_Характеристики;category id;Назва у постачальника;Назва_Характеристики;Одиниця_виміру_Характеристики;Значення_Характеристики
https://secur.ua/ajax/startovye-komplekty/status=1;...;Вага;г;;457;Бездротові GSM сигналізації;Тип;;Бездротові GSM сигналізації
"""
import csv
from pathlib import Path
from typing import Dict, List, Optional


class CategorySpecsEnricher:
    """
    Додає додаткові характеристики до товарів на основі їх категорії.

    Два методи збагачення:
      enrich_specs(specs, category_url)         → для retail (пошук за URL)
      enrich_specs_by_category_id(specs, cat_id)→ для feed   (пошук за category id)
    """

    def __init__(self, csv_path: str, supplier_id: str):
        """
        Args:
            csv_path: Шлях до CSV файлу з маппінгом категорій.
            supplier_id: Ідентифікатор постачальника (для логування).
        """
        self.csv_path = Path(csv_path)
        self.supplier_id = supplier_id

        # URL → specs  (retail)
        self.category_specs_mapping: Dict[str, List[Dict]] = {}

        # (feed_id, category_id) → specs  (feed)
        # feed_id = "" для категорій, які не прив’язані до конкретного фіду
        self.category_id_specs_mapping: Dict[tuple, List[Dict]] = {}

        self._load_mapping()

    # ------------------------------------------------------------------
    # LOADING
    # ------------------------------------------------------------------

    def _load_mapping(self) -> None:
        """
        Завантажує маппінг з CSV.

        CSV містить дві пари колонок характеристик з однаковими іменами;
        csv.DictReader при дублікатах залишає ОСТАННЄ значення. Тому ми
        читаємо raw-рядки через csv.reader і парсимо вручну щоб отримати обидва набори.
        """
        try:
            with open(self.csv_path, encoding="utf-8-sig") as f:
                raw_reader = csv.reader(f, delimiter=";")
                all_rows = list(raw_reader)

            if not all_rows:
                return

            headers = all_rows[0]

            # Визначаємо індекси потрібних колонок
            def col(name: str, start: int = 0) -> int:
                """Перший index колонки name починаючи з позиції start."""
                for i, h in enumerate(headers):
                    if i >= start and h.strip() == name:
                        return i
                return -1

            idx_url = col("Линк категории поставщика")
            idx_channel = col("channel")

            # Retail-специфічні характеристики (перший набір)
            idx_spec_name_retail = col("Назва_Характеристики")
            idx_spec_unit_retail = col("Одиниця_виміру_Характеристики")
            idx_spec_val_retail = col("Значення_Характеристики")

            # Feed-специфічні характеристики (другий набір)
            # Secur: другий набір після колонки "category id"
            # Viatec: колонки "category id" немає → другий набір відразу після першого набору
            idx_cat_id = col("category id")
            if idx_cat_id >= 0:
                # Secur-формат: є category id → шукаємо після нього
                start_after = idx_cat_id + 1
            elif idx_spec_val_retail >= 0:
                # Viatec-формат: нема category id → шукаємо після першого набору (після Значення col)
                start_after = idx_spec_val_retail + 1
            else:
                start_after = 0
            idx_spec_name_feed = col("Назва_Характеристики", start_after)
            idx_spec_unit_feed = col("Одиниця_виміру_Характеристики", start_after)
            idx_spec_val_feed = col("Значення_Характеристики", start_after)

            def safe_get(row: list, idx: int) -> str:
                if idx < 0 or idx >= len(row):
                    return ""
                return row[idx].strip().strip('"')

            for row in all_rows[1:]:
                if not row or all(c.strip() == "" for c in row):
                    continue

                channel = safe_get(row, idx_channel)
                # Обробляємо тільки site-рядки (перший канал), щоб уникнути дублікатів
                if channel != "site":
                    continue

                category_url = safe_get(row, idx_url)
                category_id = safe_get(row, idx_cat_id)

                # --- Retail specs (за URL) ---
                if category_url and category_url.startswith("http"):
                    # Перший набір (cols 12-14): значення зазвичай порожні —
                    # ці колонки використовуються FieldProcessor для конвертації одиниць ваги.
                    spec_name = safe_get(row, idx_spec_name_retail)
                    spec_unit = safe_get(row, idx_spec_unit_retail)
                    spec_value = safe_get(row, idx_spec_val_retail)

                    if spec_name and spec_value:
                        self.category_specs_mapping.setdefault(category_url, []).append(
                            {"name": spec_name, "unit": spec_unit, "value": spec_value}
                        )

                    # ✅ Другий набір (cols 18-20): Тип та інші —
                    # додаємо і по URL (retail), не тільки по category id (feed).
                    spec_name_2 = safe_get(row, idx_spec_name_feed)
                    spec_unit_2 = safe_get(row, idx_spec_unit_feed)
                    spec_value_2 = safe_get(row, idx_spec_val_feed)

                    if spec_name_2 and spec_value_2:
                        self.category_specs_mapping.setdefault(category_url, []).append(
                            {"name": spec_name_2, "unit": spec_unit_2, "value": spec_value_2}
                        )

                # --- Feed specs (за category id) ---
                if category_id:
                    # читаємо feed з відповідної колонки CSV
                    row_feed = safe_get(row, col("feed")) if "feed" in headers else ""
                    key = (row_feed, category_id)
                    if key not in self.category_id_specs_mapping:
                        spec_name = safe_get(row, idx_spec_name_feed)
                        spec_unit = safe_get(row, idx_spec_unit_feed)
                        spec_value = safe_get(row, idx_spec_val_feed)

                        if spec_name and spec_value:
                            self.category_id_specs_mapping.setdefault(key, []).append(
                                {"name": spec_name, "unit": spec_unit, "value": spec_value}
                            )

            total_url = len(self.category_specs_mapping)
            total_id = len(self.category_id_specs_mapping)
            url_specs = sum(len(v) for v in self.category_specs_mapping.values())
            id_specs = sum(len(v) for v in self.category_id_specs_mapping.values())

            print(
                f"✅ [{self.supplier_id}] Завантажено: "
                f"{total_url} URL-категорій ({url_specs} specs), "
                f"{total_id} id-категорій ({id_specs} specs)"
            )

        except FileNotFoundError:
            print(f"⚠️ [{self.supplier_id}] CSV файл не знайдено: {self.csv_path}")
        except Exception as e:
            print(f"❌ [{self.supplier_id}] Помилка завантаження маппінгу: {e}")

    # ------------------------------------------------------------------
    # PUBLIC API
    # ------------------------------------------------------------------

    def enrich_specs(self, specifications_list: List[Dict], category_url: str) -> List[Dict]:
        """
        Додає характеристики за URL категорії (retail.py).

        Args:
            specifications_list: Існуючі характеристики товару.
            category_url: URL категорії постачальника.

        Returns:
            Оновлений список характеристик.
        """
        return self._enrich(specifications_list, self.category_specs_mapping.get(category_url, []))

    def enrich_specs_by_category_id(
        self, specifications_list: List[Dict], category_id: str, feed_id: str = ""
    ) -> List[Dict]:
        """
        Додає характеристики за category id (feed.py).

        Пошук за ключом (feed_id, category_id), потім fallback ("", category_id).
        Це гарантує правильні специ для категорій, які є одночасно
        у декількох фідах (напр. 25 у фідах 50 і 52).

        Args:
            specifications_list: Існуючі характеристики товару.
            category_id: ID категорії постачальника з XML-фіду.
            feed_id: ID фіду (напр. "50" або "52"). Порожній — fallback на "".

        Returns:
            Оновлений список характеристик.
        """
        cat_id = str(category_id).strip()
        extra = (
            self.category_id_specs_mapping.get((feed_id, cat_id))
            or self.category_id_specs_mapping.get(("", cat_id))
            or []
        )
        return self._enrich(specifications_list, extra)

    # ------------------------------------------------------------------
    # HELPERS
    # ------------------------------------------------------------------

    def _enrich(self, specs: List[Dict], extra_specs: List[Dict]) -> List[Dict]:
        """
        Додає extra_specs до specs, уникаючи дублікатів за іменем.

        Вручну прописані категорійні характеристики отримують rule_priority=0
        (вище будь-якого name/attribute правила в viatec_mapping_rules.csv),
        щоб у merge_all_specs вони завжди перемагали автоматичний парсинг назви.
        Наприклад: CSV каже "Тип устройства = Відеодомофон", а назва товару
        містить слово "Аудиодомофон" — залишається значення з CSV.
        """
        if not extra_specs:
            return specs

        existing_names = {s.get("name", "").lower() for s in specs}
        enriched = list(specs)

        for spec in extra_specs:
            name = spec.get("name", "")
            if name.lower() not in existing_names:
                # rule_priority=0 — максимальний пріоритет: перемагає будь-яке
                # name-правило у merge_all_specs (навіть priority=1 у mapping_rules)
                enriched.append({
                    **spec,
                    "rule_priority": 0,
                    "rule_kind": "extract",
                })
                existing_names.add(name.lower())

        return enriched

    # ------------------------------------------------------------------
    # INTROSPECTION
    # ------------------------------------------------------------------

    def has_specs_for_category(self, category_url: str) -> bool:
        return category_url in self.category_specs_mapping

    def has_specs_for_category_id(self, category_id: str, feed_id: str = "") -> bool:
        cat_id = str(category_id).strip()
        return (
            (feed_id, cat_id) in self.category_id_specs_mapping
            or ("", cat_id) in self.category_id_specs_mapping
        )

    def get_category_specs(self, category_url: str) -> Optional[List[Dict]]:
        return self.category_specs_mapping.get(category_url)

    def get_category_id_specs(self, category_id: str, feed_id: str = "") -> Optional[List[Dict]]:
        cat_id = str(category_id).strip()
        return (
            self.category_id_specs_mapping.get((feed_id, cat_id))
            or self.category_id_specs_mapping.get(("", cat_id))
        )
