#!/usr/bin/env python3
"""
python scripts/markets_export_coef.py

Orchestrator — послідовно запускає всі market export-coef скрипти.
Лог кожного скрипту стримиться в реальному часі, як при окремому запуску.
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

SCRIPTS_DIR = Path(__file__).parent

SCRIPTS: list[str] = [
    "prom_export_coef.py",
    "kasta_export_coef.py",
    "rozetka_export_coef.py",
    "epicenter_export_coef.py",
]

SEP = "═" * 60


def run_script(path: Path) -> int:
    """Запускає скрипт, виводить stdout+stderr в реальному часі. Повертає exit code."""
    print(f"\n{SEP}\n▶  {path.name}\n{SEP}", flush=True)

    start = time.monotonic()

    proc = subprocess.Popen(
        [sys.executable, str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="", flush=True)

    proc.wait()
    elapsed = time.monotonic() - start
    status = "✅ OK" if proc.returncode == 0 else f"❌ FAILED (exit {proc.returncode})"

    print(f"{SEP}\n{status}  —  {path.name}  ({elapsed:.1f}s)\n{SEP}", flush=True)
    return proc.returncode


def main() -> None:
    results: dict[str, int] = {}

    for name in SCRIPTS:
        path = SCRIPTS_DIR / name
        if not path.exists():
            print(f"\n⚠️  {name} не знайдено — пропускаємо\n", flush=True)
            results[name] = -1
            continue
        results[name] = run_script(path)

    # ── підсумок ──────────────────────────────────────────────
    print(f"\n{SEP}\nПІДСУМОК\n{SEP}")
    failed: list[str] = []
    for name, code in results.items():
        if code == 0:
            print(f"  ✅  {name}")
        elif code == -1:
            print(f"  ⚠️  {name}  (не знайдено)")
        else:
            print(f"  ❌  {name}  (exit {code})")
            failed.append(name)
    print(SEP)

    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
