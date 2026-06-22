"""
epicenter_attr_pipeline.py
--------------------------
Послідовний запуск кроків пайплайну атрибутів Epicenter:

  1. epicenter_export_attr_sets.py    — сети атрибутів з API → xlsx
  2. epicenter_map_attributes.py      — prom_param_name (hard-mapping)
  3. epicenter_export_attr_options.py — опції атрибутів з API → xlsx
  4. epicenter_map_attr_options.py    — prom_option_name + дефолти

Зупиняється при першій помилці (ненульовий exit code або unhandled exception).
Виводить час кожного кроку і загальний час.

CLI-аргументи передаються лише у крок 4 (epicenter_map_attr_options.py):
    python scripts/epicenter_attr_pipeline.py                 # full run
    python scripts/epicenter_attr_pipeline.py --dry-run       # крок 4 без запису
    python scripts/epicenter_attr_pipeline.py --no-feed       # крок 4 без фіду
    python scripts/epicenter_attr_pipeline.py --verbose       # крок 4 verbose
    python scripts/epicenter_attr_pipeline.py --dry-run --verbose

Запуск:
    python scripts/epicenter_attr_pipeline.py
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

# ─── Config ───────────────────────────────────────────────────────────────────

SCRIPTS_DIR = Path(__file__).parent

STEPS: list[tuple[str, str]] = [
    ("Export attr sets",    "epicenter_export_attr_sets.py"),
    ("Map attributes",      "epicenter_map_attributes.py"),
    ("Export attr options", "epicenter_export_attr_options.py"),
    ("Map attr options",    "epicenter_map_attr_options.py"),   # receives CLI args
]

_SEP = "─" * 62


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _validate_scripts(paths: list[Path]) -> None:
    """Abort early if any script file is missing."""
    missing = [p for p in paths if not p.exists()]
    if missing:
        for path in missing:
            print(f"❌ Script not found: {path}", file=sys.stderr)
        sys.exit(1)


def _run_step(
    n: int,
    total: int,
    label: str,
    script: Path,
    extra_args: list[str],
) -> int:
    """
    Run one pipeline step as a subprocess.

    Inherits parent stdout/stderr so output is streamed in real-time.
    -u flag disables Python output buffering in the child process.
    Returns the subprocess exit code.
    """
    cmd = [sys.executable, "-u", str(script)] + extra_args

    # ── Header ────────────────────────────────────────────────────────────
    print(f"\n{_SEP}")
    print(f"▶  [{n}/{total}] {label}")
    print(f"   {script.name}", end="")
    if extra_args:
        print(f"  {' '.join(extra_args)}", end="")
    print(f"\n{_SEP}\n", flush=True)

    # ── Run ───────────────────────────────────────────────────────────────
    t0 = time.monotonic()
    proc = subprocess.run(cmd)
    elapsed = time.monotonic() - t0

    # ── Footer ────────────────────────────────────────────────────────────
    icon = "✅" if proc.returncode == 0 else "❌"
    print(
        f"\n{icon}  [{n}/{total}] {label}"
        f"  — {elapsed:.1f}s  (rc={proc.returncode})",
        flush=True,
    )
    return proc.returncode


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    # All CLI args forwarded verbatim to the last step only.
    extra_args: list[str] = sys.argv[1:]

    total = len(STEPS)
    scripts = [SCRIPTS_DIR / name for _, name in STEPS]

    _validate_scripts(scripts)

    # ── Plan ──────────────────────────────────────────────────────────────
    print(f"\n🚀 epicenter_attr_pipeline — {total} кроки")
    for i, (label, name) in enumerate(STEPS, 1):
        suffix = f"  ← CLI: {' '.join(extra_args)}" if (i == total and extra_args) else ""
        print(f"   {i}. {name}{suffix}")

    pipeline_start = time.monotonic()

    # ── Steps ─────────────────────────────────────────────────────────────
    try:
        for n, ((label, _), script) in enumerate(zip(STEPS, scripts), 1):
            step_extra = extra_args if n == total else []
            rc = _run_step(n, total, label, script, step_extra)
            if rc != 0:
                elapsed = time.monotonic() - pipeline_start
                print(
                    f"\n💥 Зупинено на кроці {n}/{total}: {label}"
                    f"  ({elapsed:.1f}s total)",
                    flush=True,
                )
                sys.exit(rc)

    except KeyboardInterrupt:
        elapsed = time.monotonic() - pipeline_start
        print(f"\n\n⚠️  Перервано (Ctrl+C)  — {elapsed:.1f}s total", flush=True)
        sys.exit(130)

    # ── Done ──────────────────────────────────────────────────────────────
    total_elapsed = time.monotonic() - pipeline_start
    print(f"\n🏁 Усі {total} кроки завершено — {total_elapsed:.1f}s total\n")


if __name__ == "__main__":
    main()
