"""
Утиліти для роботи з характеристиками товарів
"""
from typing import List, Dict


def should_replace_attribute(new_kind: str, new_priority: int, 
                            current_kind: str, current_priority: int) -> bool:
    """
    Визначає чи треба замінити характеристику на нову
    
    Args:
        new_kind: Тип нового правила (extract, normalize, derive, fallback, skip)
        new_priority: Пріоритет нового правила (менше = важливіше)
        current_kind: Тип поточного правила
        current_priority: Пріоритет поточного правила
    
    Returns:
        True якщо треба замінити, False якщо залишити поточне
    """
    if new_kind in ['skip', 'fallback']:
        return False
    
    if new_kind == 'derive':
        return current_kind == 'derive' and new_priority < current_priority
    
    return new_priority < current_priority


def merge_specs(target: Dict[str, Dict], specs: List[Dict], logger=None) -> None:
    """
    Зливає список характеристик у цільовий словник з урахуванням пріоритетів та rule_kind
    
    Args:
        target: Цільовий словник {normalized_name: spec_dict}
        specs: Список характеристик для злиття
        logger: Scrapy logger для дебагу (опціонально)
    
    Модифікує target in-place.
    
    Example:
        >>> target = {}
        >>> merge_specs(target, [{'name': 'Тип', 'value': 'UTP', 'rule_priority': 10}])
        >>> merge_specs(target, [{'name': 'тип', 'value': 'FTP', 'rule_priority': 5}])
        >>> target['тип']['value']
        'FTP'  # Замінилось через вищий пріоритет (5 < 10)
    """
    for spec in specs:
        rule_kind = spec.get('rule_kind', 'extract')
        
        # Пропускаємо skip
        if rule_kind == 'skip':
            if logger:
                logger.debug(f"⏭️ Пропущено (skip): {spec.get('name')}")
            continue
        
        key = spec.get('name', '').lower().strip()
        if not key:
            continue
        
        # Якщо атрибут ще не існує - додаємо
        if key not in target:
            target[key] = spec
            if logger:
                logger.debug(
                    f"➕ Додано: {spec.get('name')} = {spec.get('value')} "
                    f"[{rule_kind}, priority={spec.get('rule_priority', 999)}]"
                )
            continue
        
        # Атрибут існує - перевіряємо чи треба замінити
        current = target[key]
        current_kind = current.get('rule_kind', 'extract')
        current_priority = current.get('rule_priority', 999)
        new_priority = spec.get('rule_priority', 999)
        
        if should_replace_attribute(rule_kind, new_priority, current_kind, current_priority):
            target[key] = spec
            if logger:
                logger.debug(
                    f"🔄 Замінено: {spec.get('name')}: "
                    f"{current_kind}[{current_priority}] → {rule_kind}[{new_priority}]: "
                    f"{spec.get('value')}"
                )
        else:
            if logger:
                logger.debug(
                    f"⏭️ Пропущено (пріоритет): {spec.get('name')}: "
                    f"rule_kind={rule_kind}, current={current_kind}[{current_priority}], "
                    f"new=[{new_priority}]"
                )


def merge_all_specs(supplier_specs: List[Dict], mapped_specs: List[Dict], 
                   name_specs: List[Dict], logger=None) -> List[Dict]:
    """
    Зливає характеристики з усіх джерел у фінальний список
    
    Args:
        supplier_specs: Оригінальні характеристики постачальника
        mapped_specs: Змаплені характеристики через правила
        name_specs: Характеристики витягнуті з назви товару
        logger: Scrapy logger
    
    Returns:
        Список фінальних характеристик після дедуплікації та пріоритизації
    """
    specs_dict = {}
    
    # Порядок важливий: спочатку supplier (найнижчий пріоритет)
    merge_specs(specs_dict, supplier_specs, logger)
    merge_specs(specs_dict, mapped_specs, logger)
    merge_specs(specs_dict, name_specs, logger)
    
    return list(specs_dict.values())
