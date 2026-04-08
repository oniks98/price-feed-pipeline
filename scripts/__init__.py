"""
Ініціалізація модуля scripts.
Автоматично додає кореневу директорію проекту до sys.path.
"""
import sys
from pathlib import Path

# Додаємо кореневу директорію проекту до sys.path
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
