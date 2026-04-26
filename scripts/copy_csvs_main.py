"""
Витягує *_old.csv з гілки data-latest в поточну робочу директорію (main).
Потрібно для локального тестування generate_*_feed.py без переключення гілок.

Запуск:
    python scripts/copy_csvs_main.py
"""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).parents[1]
BRANCH = "data-latest"
SUPPLIERS = ["viatec", "secur"]


def git_show(branch: str, git_path: str, dest: Path) -> bool:
    """Витягує файл з іншої гілки через `git show`. Повертає True при успіху."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        result = subprocess.run(
            ["git", "show", f"{branch}:{git_path}"],
            capture_output=True,
            cwd=ROOT,
        )
        if result.returncode != 0:
            print(f"❌ git show {branch}:{git_path} → {result.stderr.decode().strip()}")
            return False
        dest.write_bytes(result.stdout)
        size_kb = len(result.stdout) / 1024
        print(f"✅ {dest.relative_to(ROOT)}  ({size_kb:.0f} KB)")
        return True
    except FileNotFoundError:
        print("❌ git не знайдено — запустіть скрипт з Git Bash або додайте git до PATH")
        sys.exit(1)


def main() -> None:
    print(f"📥 Витягуємо *_old.csv з гілки '{BRANCH}'...\n")
    ok = 0
    for supplier in SUPPLIERS:
        git_path = f"data/{supplier}/{supplier}_old.csv"
        dest = ROOT / "data" / supplier / f"{supplier}_old.csv"
        if git_show(BRANCH, git_path, dest):
            ok += 1

    print(f"\n{'✅ Готово' if ok == len(SUPPLIERS) else '⚠️  Частково'}: {ok}/{len(SUPPLIERS)} файлів отримано")
    if ok < len(SUPPLIERS):
        print(f"   Переконайтесь що гілка '{BRANCH}' існує і містить *_old.csv")


if __name__ == "__main__":
    main()
