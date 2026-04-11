"""
Загальний сервіс збагачення характеристик товару.

Відповідає за додавання стандартних характеристик, які не залежать
від категорії або постачальника, але є обов'язковими для кожного товару:

  • "Стан"              — завжди "Новий", якщо відсутнє
  • "Компанія-виробник" — з поля Виробник, якщо відсутнє в specs
  • "Країна-виробник"   — з поля Країна_виробник, якщо відсутнє в specs
"""

from __future__ import annotations


class SpecsEnricher:
    """
    Збагачує список характеристик стандартними обов'язковими полями.

    Усі методи — stateless, приймають список specs і повертають новий список.
    Порядок застосування у pipeline:
        specs = SpecsEnricher.ensure_condition(specs)
        specs = SpecsEnricher.ensure_manufacturer_specs(specs, cleaned)
    """

    @staticmethod
    def ensure_condition(specs: list[dict]) -> list[dict]:
        """
        Додає {"name": "Стан", "unit": "", "value": "Новий"} якщо відсутнє.

        Дедуплікація відбувається за ключем "name" (case-insensitive, trim).
        """
        specs_dict: dict[str, dict] = {
            s["name"].lower().strip(): s for s in specs
        }
        if "стан" not in specs_dict:
            specs_dict["стан"] = {"name": "Стан", "unit": "", "value": "Новий"}
        return list(specs_dict.values())

    @staticmethod
    def ensure_manufacturer_specs(specs: list[dict], cleaned: dict) -> list[dict]:
        """
        Додає характеристики "Компанія-виробник" та "Країна-виробник"
        зі значень полів cleaned["Виробник"] та cleaned["Країна_виробник"].

        Логіка:
          - Характеристику додаємо тільки якщо:
              1. Значення в cleaned не порожнє
              2. Характеристики з таким іменем ще немає в specs
          - Дедуплікація за іменем (case-insensitive, trim)
        """
        existing: dict[str, dict] = {
            s["name"].lower().strip(): s for s in specs
        }

        manufacturer = (cleaned.get("Виробник") or "").strip()
        country = (cleaned.get("Країна_виробник") or "").strip()

        if manufacturer and "компанія-виробник" not in existing:
            existing["компанія-виробник"] = {
                "name": "Компанія-виробник",
                "unit": "",
                "value": manufacturer,
            }

        if country and "країна-виробник" not in existing:
            existing["країна-виробник"] = {
                "name": "Країна-виробник",
                "unit": "",
                "value": country,
            }

        return list(existing.values())

    @staticmethod
    def enrich(specs: list[dict], cleaned: dict) -> list[dict]:
        """
        Застосовує всі стандартні збагачення в правильному порядку.

        Зручний фасад: замість двох окремих викликів — один.

        Args:
            specs:   список характеристик після AttributeMapper / merge_all_specs
            cleaned: підготовлений item-словник (вже з заповненими Виробник / Країна_виробник)

        Returns:
            Збагачений список характеристик.
        """
        specs = SpecsEnricher.ensure_condition(specs)
        specs = SpecsEnricher.ensure_manufacturer_specs(specs, cleaned)
        return specs
