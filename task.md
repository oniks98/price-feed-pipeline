делай исправление прямо в C:\FullStack\PriceFeedPipeline\scripts\generate_epicenter_feed.py

Правильное именование и разделение по типам
Логика дефолта зависит от attr_type:
attr_typeДефолт берётся изoption_code в фидеfloat, text, arrayoption_name_ukне передаётсяselect, multiselectdefault_option_codeобязателен

Переименование и расширение
pythonfrom dataclasses import dataclass, field
from typing import Final, Literal

AttrType = Literal["float", "text", "array", "select", "multiselect"]

@dataclass(frozen=True)
class AttrConfig:
attr_code: str
attr_type: AttrType
attr_name_uk: str # None → Prom-источника нет, сразу идём в дефолт
prom_param_name: str | None
prom_aliases: tuple[str, ...] = ()

Резолвинг дефолта — по attr_type
python@dataclass(frozen=True)
class AttrDefaults: # для float / text / array
option_name_uk: dict[str, str] = field(default_factory=dict) # attr_code → значение # для select / multiselect
option_code: dict[str, str] = field(default_factory=dict) # attr_code → code

def resolve_attr_value(
cfg: AttrConfig,
prom_params: dict[str, str],
defaults: AttrDefaults,
) -> dict[str, str] | None:
"""
Возвращает готовый payload для <param> или None (→ drop).

    float/text/array : value = option_name_uk,  option_code не передаём
    select/multiselect: value = option_name_uk, option_code обязателен
    """
    raw_value: str | None = None

    # 1. Ищем в Prom
    if cfg.prom_param_name is not None:
        for key in (cfg.prom_param_name, *cfg.prom_aliases):
            if found := prom_params.get(key):
                raw_value = found
                break

    # 2. Fallback по типу
    if raw_value is None:
        if cfg.attr_type in ("float", "text", "array"):
            raw_value = defaults.option_name_uk.get(cfg.attr_code)
        else:
            # select / multiselect — дефолт через option_code, не option_name_uk
            option_code = defaults.option_code.get(cfg.attr_code)
            if option_code is None:
                logger.warning("attr drop | no default_option_code | attr_code=%s", cfg.attr_code)
                return None
            return {
                "paramcode": cfg.attr_code,
                "name": cfg.attr_name_uk,
                "valuecode": option_code,
                # value (label) можно добавить если есть маппинг code→name
            }

    if raw_value is None:
        logger.warning("attr drop | no value and no default | attr_code=%s", cfg.attr_code)
        return None

    return {
        "paramcode": cfg.attr_code,
        "name": cfg.attr_name_uk,
        "value": raw_value,
        # option_code намеренно отсутствует для float/text/array
    }

Реестр атрибутов
python_ATTRS: Final[tuple[AttrConfig, ...]] = ( # --- float: нет prom_param_name → сразу option_name_uk ---
AttrConfig(
attr_code="ratio",
attr_type="float",
attr_name_uk="Мінімальна кратність товару",
prom_param_name=None,
), # --- float: источник Prom "Розміри" → fallback option_name_uk ---
AttrConfig(attr_code="weight", attr_type="float", attr_name_uk="Вага", prom_param_name="Вага"),
AttrConfig(attr_code="width", attr_type="float", attr_name_uk="Ширина", prom_param_name="Розміри"),
AttrConfig(attr_code="height", attr_type="float", attr_name_uk="Висота", prom_param_name="Розміри"),
AttrConfig(
attr_code="length",
attr_type="float",
attr_name_uk="Глибина",
prom_param_name="Розміри",
prom_aliases=("Довжина",),
), # --- select: дефолт через default_option_code ---
AttrConfig(
attr_code="measure",
attr_type="select",
attr_name_uk="Одиниця виміру та кількість",
prom_param_name=None, # всегда из default_option_code
),
AttrConfig(
attr_code="country_of_origin",
attr_type="select",
attr_name_uk="Країна-виробник",
prom_param_name="Країна виробник",
), # --- multiselect ---
AttrConfig(
attr_code="brand",
attr_type="multiselect",
attr_name_uk="Бренд",
prom_param_name="Бренд",
),
)

Что изменилось
БылоСталоFloatAttrConfig — только floatAttrConfig + поле attr_type — все типыОдин путь к дефолтуfloat/text/array → option_name_uk, select/multiselect → option_codeЛогика размазанаresolve_attr_value — единая точка входа для любого типа

Now I have a full picture of both files. The document describes a refactoring of the attribute config system. Let me understand what needs to change:

New dataclasses: AttrConfig (with attr_code, attr_type, attr_name_uk, prom_param_name, prom_aliases) and AttrDefaults (with option_name_uk dict and option_code dict).
AttrType literal for typing.
resolve_attr_value function — unified entry point for all attr types.
\_ATTRS registry — replaces \_SYSTEM_NUMERIC in generate_epicenter_feed.py.
The key change is: \_SYSTEM_NUMERIC dict in generate_epicenter_feed.py gets replaced by \_ATTRS registry using AttrConfig dataclass, and the resolution logic in inject_epicenter_attrs gets simplified using resolve_attr_value.

The document specifies these changes as improvements to the naming and separation by type. Looking at what's in generate_epicenter_feed.py:

\_SYSTEM_NUMERIC dict maps prom_param_name → (epicenter_paramcode, epicenter_name) — only for float/numeric types
Step 5a processes these system numeric attrs
Steps 5b and 5c handle measure (select) and category attrs from xlsx

The proposed change:

Replace \_SYSTEM_NUMERIC with \_ATTRS: Final[tuple[AttrConfig, ...]]
Add AttrConfig, AttrType, AttrDefaults dataclasses
Add resolve_attr_value function
Refactor inject_epicenter_attrs step 5a to use the new registry

But wait — looking more carefully at the document's \_ATTRS registry, it includes attrs like ratio, weight, width, height, length, measure, country_of_origin, brand. Some of these overlap with what's currently in \_SYSTEM_NUMERIC and some are handled via option_map/defaults from xlsx.
The document says measure stays as-is (via \_resolve_measure), and country_of_origin/brand stay via option_map. So the \_ATTRS registry in the document is more of a conceptual illustration than a 1:1 replacement.
…Show lessLoading toolsResultNow I have Filesystem:edit_file. Let me apply the changes one by one.
Here's my plan for the changes:
Edit 1: Imports — add dataclass, field and Literal
Edit 2: System attributes config section — replace the entire \_SYSTEM_NUMERIC block + comment with:

AttrType Literal
AttrConfig dataclass
AttrDefaults dataclass
resolve_attr_value function
\_ATTRS registry

Edit 3: Add \_render_attr_payload — new helper after \_render_system_numeric
Edit 4: Remove \_resolve_measure function
Edit 5: Update inject_epicenter_attrs:

Change local vars
Replace steps 5a + 5b

Let me now write each edit carefully.
For \_ATTRS, based on current \_SYSTEM_NUMERIC:

"Ширина" → width (float)
"Висота" → height (float)
"Довжина" → length (float), "Глибина" is also an alias
"Глибина" → length (float) - this is a second key in \_SYSTEM_NUMERIC that maps to the same code
"Вага" → weight (float)
"Кратність" → ratio (float)
measure → select (no prom source)

For length, prom_param_name="Глибина" (the primary Epicenter name) and prom_aliases=("Довжина",) (the Prom alias). Actually wait, in the current \_SYSTEM_NUMERIC:
python"Довжина": ("length", "Глибина"), # Prom="Довжина" → Epicenter="Глибина"
"Глибина": ("length", "Глибина"),
Both "Довжина" and "Глибина" are Prom param names that map to the same Epicenter attr. So prom_param_name="Довжина" and prom_aliases=("Глибина",) would make sense — or either one as primary with the other as alias.
Let me use prom_param_name="Глибина" (primary) with prom_aliases=("Довжина",) as that matches the existing pattern where "Глибина" is the Epicenter name.
Wait, actually "Глибина" is the Epicenter name but IS it also a Prom param? Looking at the current code, YES — \_SYSTEM_NUMERIC has "Глибина" as a key meaning it IS a Prom param. …Show less
