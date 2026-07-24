"""
Сервіс контролю кількості характеристик для Prom.ua.

Prom.ua відхиляє товари з кількістю характеристик > PROM_HARD_LIMIT (100).

Відповідальності модуля:
  • PROM_HARD_LIMIT — єдина точка правди для ліміту Prom
  • SpecLimitService.apply_limit() — фінальний захисний обрізувач перед _write_row
    з гарантованим збереженням обов'язкових характеристик

Обов'язкові характеристики (_PROTECTED_NAMES) ніколи не відкидаються,
навіть якщо загальний список перевищує ліміт.
Вони додаються pipeline автоматично через SpecsEnricher та RequiredGuaranteeService
(для категорій з REQUIRED_GUARANTEE_CATEGORIES, див. required_guarantee.py).

ДВОШАРОВА АРХІТЕКТУРА ОБМЕЖЕННЯ (реалізована в pipelines.py):

  Шар 1 — Семантична дедуплікація (в _process_specs):
    Якщо full_specs > PROM_HARD_LIMIT:
      замінюємо (supplier_raw + mapped) на (unmapped + mapped).
      Інформація не втрачається — mapped вже є нормалізованими Prom-версіями.

  Шар 2 — Захисний обрізувач (перед _write_row):
    SpecLimitService.apply_limit() — hard cap з гарантією protected specs.
    Спрацьовує тільки якщо після Шару 1 все ще > ліміту.
"""

from __future__ import annotations

# ────────────────────────────────────────────────────────────────────
# Константи
# ────────────────────────────────────────────────────────────────────

PROM_HARD_LIMIT: int = 100  # Максимум характеристик на товар у Prom.ua
PROM_CSV_SPECS_LIMIT: int = PROM_HARD_LIMIT + 1  # Слотів у CSV-заголовку (=101); +1 — резервний відступ

# Назви обов'язкових характеристик (нижній регістр, trim).
# Додаються pipeline автоматично через SpecsEnricher і ніколи не обрізаються.
_PROTECTED_NAMES: frozenset[str] = frozenset({
    "стан",
    "компанія-виробник",
    "країна-виробник",
    "гарантійний термін",
})


# ────────────────────────────────────────────────────────────────────
# Сервіс
# ────────────────────────────────────────────────────────────────────

class SpecLimitService:
    """
    Фінальний захисний обрізувач кількості характеристик перед записом у CSV.

    Гарантує:
      • результат містить ≤ PROM_HARD_LIMIT характеристик
      • захищені характеристики (Стан, Компанія-виробник, Країна-виробник)
        ніколи не відкидаються незалежно від їх позиції у списку

    Використовується як Шар 2 після семантичної дедуплікації (Шар 1 в _process_specs).

    Патерн використання:
        specs = SpecLimitService.apply_limit(specs, spider.logger, product_name)
        self._write_row(output_file, cleaned, specs)
    """

    @staticmethod
    def apply_limit(
        specs: list[dict],
        logger=None,
        product_name: str = "",
    ) -> list[dict]:
        """
        Обмежує кількість характеристик до PROM_HARD_LIMIT.

        Алгоритм:
          1. Якщо кількість ≤ ліміту — повертає без змін (fast path).
          2. Розділяє на protected (Стан, Виробник, Країна) і regular.
          3. Бере перші (PROM_HARD_LIMIT − len(protected)) regular.
          4. Повертає regular[:slots] + protected.

        Args:
            specs:        Список характеристик після всього постпроцесингу.
            logger:       Scrapy logger для попереджень (опціонально).
            product_name: Назва товару для діагностичного логу.

        Returns:
            Список характеристик розміром ≤ PROM_HARD_LIMIT.
            Захищені характеристики присутні завжди.
        """
        if len(specs) <= PROM_HARD_LIMIT:
            return specs

        protected: list[dict] = []
        regular: list[dict] = []

        for spec in specs:
            key = spec.get("name", "").lower().strip()
            (protected if key in _PROTECTED_NAMES else regular).append(spec)

        slots = PROM_HARD_LIMIT - len(protected)
        dropped = len(regular) - max(slots, 0)

        if logger:
            label = product_name[:60] if product_name else "?"
            logger.warning(
                f"⚠️ [SpecLimit] [{label}] "
                f"total={len(specs)} > {PROM_HARD_LIMIT} → "
                f"regular {len(regular)} → {max(slots, 0)}, "
                f"protected={len(protected)}, dropped={dropped}"
            )

        return regular[:max(slots, 0)] + protected
