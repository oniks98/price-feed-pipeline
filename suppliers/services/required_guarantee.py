"""
Мінімальний гарантійний термін для окремих Prom-категорій.

Розетка вимагає, щоб товари у визначених Prom-категоріях (ДБЖ, акумулятори
загального призначення, повербанки/зарядні станції) завжди мали
характеристику "Гарантійний термін". Від постачальників вона приходить
приблизно у 80% товарів — мапиться з "Гарантія, міс" / "Строк гарантії"
через AttributeMapper (mapping_rules.csv). Для решти товарів (де такої
хар-ки немає взагалі) — проставляємо дефолт.

Правило працює на рівні пайплайна, ОДНАКОВО для всіх каналів
(site/prom/rozetka/...) — ключем є тільки category_id
(Ідентифікатор_підрозділу), канал НЕ враховується.

Викликати ПІСЛЯ AttributeMapper / _process_specs, тобто на вже змаплених
specs, а не на сирих назвах постачальника. Так дефолт коректно спрацьовує
і для нових постачальників, чиї сирі назви гарантії ще не додані у
mapping_rules.csv — перевіряється канонічне ім'я, а не список синонімів.

Якщо хар-ка вже присутня — значення НЕ чіпаємо (навіть якщо воно менше 6):
поточна вимога — "додати, якщо відсутнє", без апгрейду наявних значень.
"""

from __future__ import annotations

_SPEC_NAME = "Гарантійний термін"
_SPEC_UNIT = "міс"
_SPEC_VALUE = "6"

# Prom-категорії, для яких Розетка вимагає обов'язкову "Гарантійний термін",
# разом з людською назвою (використовується тільки в підсумковому лозі).
# Єдине місце правки, якщо перелік категорій або мінімальний строк зміняться.
REQUIRED_GUARANTEE_CATEGORIES: dict[str, str] = {
    "14191106": "Блоки живлення > Джерела безперебійного живлення (ДБЖ)",
    "5280501": "Батареї та акумулятори > Акумулятори загального призначення",
    "500901": "Повербанки та зарядні станції > Зарядні станції",
}


class RequiredGuaranteeService:
    """
    Гарантує наявність "Гарантійний термін" для товарів у
    REQUIRED_GUARANTEE_CATEGORIES.

    Stateless: приймає specs і category_id, повертає (specs, defaulted).
    defaulted=True тільки коли характеристику щойно додано — pipeline
    використовує цей прапорець для підрахунку статистики (не парсить specs
    вдруге, щоб дізнатись, спрацювало правило чи ні).

    Патерн використання (pipelines.py, одразу після визначення category_id):
        category_id = channel_config.subdivision_id
        specs, defaulted = RequiredGuaranteeService.ensure_guarantee(specs, category_id)
        if defaulted:
            self._inc_guarantee(output_file, category_id)
        ...
        # наприкінці run, у close_spider:
        RequiredGuaranteeService.log_summary(self.stats[file]["guarantee_defaults"], spider.logger)
    """

    @staticmethod
    def ensure_guarantee(specs: list[dict], category_id: str) -> tuple[list[dict], bool]:
        """
        Додає {"name": "Гарантійний термін", "unit": "міс", "value": "6"},
        якщо category_id входить у REQUIRED_GUARANTEE_CATEGORIES і хар-ка
        ще відсутня в specs. Інакше повертає specs без змін (fast path).
        """
        cat = str(category_id).strip()
        if cat not in REQUIRED_GUARANTEE_CATEGORIES:
            return specs, False

        exists = any(
            s.get("name", "").strip().lower() == _SPEC_NAME.lower()
            for s in specs
        )
        if exists:
            return specs, False

        new_specs = specs + [{
            "name": _SPEC_NAME,
            "unit": _SPEC_UNIT,
            "value": _SPEC_VALUE,
        }]
        return new_specs, True

    @staticmethod
    def log_summary(counts: dict[str, int], logger) -> None:
        """
        Друкує підсумок один раз наприкінці run (close_spider) — по одному
        рядку на категорію, у якій спрацював дефолт. Мовчить, якщо порожньо
        (жоден товар не потребував дефолту) — без шуму в логах.

        Формат: "category_id — назва: N товар(ів)".
        """
        if not counts:
            return
        for category_id, count in sorted(counts.items()):
            name = REQUIRED_GUARANTEE_CATEGORIES.get(category_id, "?")
            logger.info(
                f"🛡️ Гарантія за замовчуванням (6 міс): "
                f"{category_id} — {name}: {count} товар(ів)"
            )
