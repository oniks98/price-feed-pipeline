"""
Генератор ключових слів для камер відеоспостереження.
Категорія: 301105
"""

import re
from typing import List, Set, Optional

from keywords.core.helpers import SpecAccessor
from keywords.utils.spec_helpers import is_spec_allowed


def generate(
    accessor: SpecAccessor,
    lang: str,
    base: str,
    allowed: Set[str]
) -> List[str]:
    """
    Генерація ключових слів для камер відеоспостереження.
    
    Повертає ТІЛЬКИ Блок 2 (характеристики).
    Блок 1 (модель/бренд) і Блок 3 (universal phrases) додаються GenericProcessor.

    Args:
        accessor: Accessor для характеристик
        lang: Мова (ru/ua)
        base: Базове ключове слово
        allowed: Множина дозволених характеристик

    Returns:
        Список ключових слів (тільки характеристики)
    """
    keywords = []

    # 1. Роздільна здатність
    if is_spec_allowed("Роздільна здатність (Мп)", allowed):
        resolution = _get_resolution(accessor)
        if resolution:
            keywords.extend([
                f"{base} {resolution}",
                f"{resolution} {base}",
                f"{base} {resolution.replace('mp', 'мп')}",
                f"{resolution.replace('mp', 'мп')} {base}"
            ])

    # 2. Тип камери (технологія: IP, AHD, TVI, CVI)
    if is_spec_allowed("Тип камери", allowed):
        tech = _get_camera_technology(accessor)
        if tech:
            tech_lower = tech.lower()
            if tech_lower == "ip":
                keywords.extend([
                    f"ip {base}",
                    f"айпи {base}",
                    f"сетевая {base}" if lang == "ru" else f"мережева {base}"
                ])
            elif tech_lower in ["tvi", "cvi", "ahd"]:
                keywords.append(f"{tech_lower} {base}")

    # 3. Форм-фактор (купольна, поворотна, циліндрична)
    if is_spec_allowed("Форм-фактор", allowed):
        camera_type = _get_camera_type(accessor, lang)
        if camera_type:
            keywords.append(f"{camera_type} {base}")

    # 4. Фокусна відстань
    if is_spec_allowed("Фокусна відстань", allowed):
        focal = _get_focal_length(accessor)
        if focal:
            keywords.append(f"{base} {focal}")

    # 5. Захист від води і пилу (IP65-68 = вулична)
    if is_spec_allowed("Захист обладнання від води і пилу IP", allowed):
        ip_rating = _get_ip_rating(accessor, lang)
        if ip_rating:
            keywords.append(f"{ip_rating} {base}")

    # 6. Бездротовий інтерфейс (WiFi)
    if is_spec_allowed("Бездротовий інтерфейс", allowed):
        wifi_value = accessor.value("Бездротовий інтерфейс")
        if wifi_value and wifi_value.lower() in {"так", "yes", "true", "є"}:
            keywords.extend([
                f"wifi {base}",
                f"вай фай {base}",
                f"{base} wifi",
                f"{base} вай фай"
            ])

    # 7. Кут огляду (широкоутна якщо > 90°)
    if is_spec_allowed("Кут огляду по горизонталі", allowed):
        if _check_wide_angle(accessor):
            if lang == "ru":
                keywords.append(f"широкоугольная {base}")
            else:
                keywords.append(f"ширококутна {base}")

    # 8. Вбудований мікрофон
    if is_spec_allowed("Вбудований мікрофон", allowed):
        mic_value = accessor.value("Вбудований мікрофон")
        if mic_value and mic_value.lower() in {"так", "yes", "true", "є", "вбудований"}:
            if lang == "ru":
                keywords.append(f"{base} с микрофоном")
            else:
                keywords.append(f"{base} з мікрофоном")

    # 9. Порт для SD-карти (запис на карту)
    if is_spec_allowed("Порт для SD-карти", allowed):
        sd_value = accessor.value("Порт для SD-карти")
        if sd_value and sd_value.lower() in {"так", "yes", "true", "є"}:
            if lang == "ru":
                keywords.append(f"{base} с записью")
            else:
                keywords.append(f"{base} з записом")

    return keywords


def _get_resolution(accessor: SpecAccessor) -> Optional[str]:
    """Витягування роздільної здатності"""
    value = accessor.value("Роздільна здатність (Мп)")
    if not value:
        return None

    # Варіант 1: Цифра + mp/мп ("2mp", "5 мп")
    match = re.search(r"(\d+)\s*[mм][pр]", value, re.I)
    if match:
        return f"{match.group(1)}mp"

    # Варіант 2: Просто цифра ("2", "5")
    match = re.search(r"^(\d+)$", value.strip())
    if match:
        return f"{match.group(1)}mp"

    return None


def _get_focal_length(accessor: SpecAccessor) -> Optional[str]:
    """Витягування фокусної відстані"""
    value = accessor.value("Фокусна відстань")
    if not value:
        return None

    # Якщо значення вже містить мм, витягуємо число
    match = re.search(r"(\d+(?:\.\d+)?)\s*(мм|mm)", value, re.I)
    if match:
        return f"{match.group(1)} мм"

    # Якщо просто число - додаємо "мм"
    match = re.search(r"^(\d+(?:\.\d+)?)$", value.strip())
    if match:
        return f"{match.group(1)} мм"

    return None


def _get_camera_technology(accessor: SpecAccessor) -> Optional[str]:
    """Витягування технології камери (IP, AHD, TVI, CVI)"""
    value = accessor.value("Тип камери")
    if not value:
        return None

    value_lower = value.lower()
    
    # Імпортуємо CAMERA_TECHNOLOGIES з models
    from keywords.core.models import CAMERA_TECHNOLOGIES
    
    for tech in CAMERA_TECHNOLOGIES:
        if tech in value_lower:
            return tech.upper()

    return None


def _get_camera_type(accessor: SpecAccessor, lang: str) -> Optional[str]:
    """Витягування типу камери (купольна/поворотна/циліндрична)"""
    value = accessor.value("Форм-фактор")
    if not value:
        return None

    mapping = {
        "ru": {
            "купол": "купольная",
            "ptz": "поворотная",
            "поворот": "поворотная",
            "циліндр": "цилиндрическая",
            "куб": "кубическая",
        },
        "ua": {
            "купол": "купольна",
            "ptz": "поворотна",
            "поворот": "поворотна",
            "циліндр": "циліндрична",
            "куб": "кубічна",
        },
    }

    value_lower = value.lower()
    for keyword, result in mapping[lang].items():
        if keyword in value_lower:
            return result

    return None


def _get_ip_rating(accessor: SpecAccessor, lang: str) -> Optional[str]:
    """Перевірка захисту IP65-68 (вулична камера)"""
    value = accessor.value("Захист обладнання від води і пилу IP")
    if not value:
        return None

    if re.search(r"ip6[5-8]", value, re.I):
        return "уличная" if lang == "ru" else "вулична"

    return None


def _check_wide_angle(accessor: SpecAccessor) -> bool:
    """Перевірка широкого кута огляду (>90 градусів)"""
    value = accessor.value("Кут огляду по горизонталі")
    if not value:
        return False

    # Шукаємо число
    match = re.search(r"(\d+)", value)
    if match:
        angle = int(match.group(1))
        return angle > 90

    return False
