#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Сервіс читання/запису статусу імпорту Prom.ua.

Файл: data/prom_import_status.json (зберігається в data-latest)

Структура:
{
  "status": "success" | "failed",
  "last_attempt_utc": "2026-04-20T18:00:00Z",
  "consecutive_failures": 0,
  "pending_hashes": [
    {"hash": "abc111", "run_utc": "2026-04-20T10:00:00Z"}
  ]
}

Правила:
  - pending_hashes — черга версій merged.csv які Prom ще не отримав.
  - При success: consecutive_failures=0, pending_hashes=[].
  - При failed:  consecutive_failures+=1, hash поточного run додається в pending_hashes.
  - merge_pending.py читає статус перед merge і об'єднує дані якщо є pending.
  - Файл амендиться в data-latest разом з merged.csv — окремих комітів немає.

Єдина відповідальність: читання і запис статус-файлу. Бізнес-логіка — в caller.
"""

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STATUS_FILENAME = "prom_import_status.json"

_EMPTY_STATUS: dict[str, Any] = {
    "status": "success",
    "last_attempt_utc": None,
    "consecutive_failures": 0,
    "pending_hashes": [],
}


def _status_path() -> Path:
    root = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
    return root / "data" / STATUS_FILENAME


def _publish_status_path() -> Path:
    """Шлях в publish-dir (клон data-latest), якщо він існує."""
    root = Path(os.environ.get("PROJECT_ROOT", r"C:\FullStack\PriceFeedPipeline"))
    publish = root.parent / "publish-dir" / "data" / STATUS_FILENAME
    return publish


def load_status() -> dict[str, Any]:
    """
    Завантажує статус з файлу.
    Спочатку шукає в publish-dir (data-latest clone), потім у data/.
    Повертає порожній статус якщо файл не знайдено або пошкоджено.
    """
    for path in (_publish_status_path(), _status_path()):
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                # Гарантуємо наявність всіх ключів (backward compat)
                return {**_EMPTY_STATUS, **data}
            except Exception as e:
                print(f"⚠️  prom_status: не вдалося прочитати {path}: {e}")

    print("ℹ️  prom_status: файл статусу не знайдено — повертаємо порожній статус")
    return dict(_EMPTY_STATUS)


def save_status(status: dict[str, Any]) -> None:
    """
    Зберігає статус в data/prom_import_status.json.
    Копія в publish-dir оновлюється якщо publish-dir існує.
    """
    payload = json.dumps(status, ensure_ascii=False, indent=2)

    local_path = _status_path()
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text(payload, encoding="utf-8")
    print(f"✅ prom_status: збережено → {local_path}")

    publish = _publish_status_path()
    if publish.parent.exists():
        publish.write_text(payload, encoding="utf-8")
        print(f"✅ prom_status: збережено → {publish}")


def compute_file_hash(file_path: Path) -> str:
    """MD5 хеш файлу для ідентифікації версії merged.csv."""
    h = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception as e:
        print(f"⚠️  prom_status: не вдалося обчислити хеш {file_path}: {e}")
        return "unknown"


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def record_success(status: dict[str, Any]) -> dict[str, Any]:
    """Повертає новий статус після успішного імпорту."""
    return {
        "status": "success",
        "last_attempt_utc": now_utc(),
        "consecutive_failures": 0,
        "pending_hashes": [],
    }


def record_failure(status: dict[str, Any], merged_hash: str) -> dict[str, Any]:
    """
    Повертає новий статус після невдалого імпорту.
    Додає поточний хеш у pending_hashes якщо він ще не присутній.
    """
    pending = list(status.get("pending_hashes") or [])

    existing_hashes = {entry["hash"] for entry in pending}
    if merged_hash not in existing_hashes:
        pending.append({"hash": merged_hash, "run_utc": now_utc()})

    return {
        "status": "failed",
        "last_attempt_utc": now_utc(),
        "consecutive_failures": status.get("consecutive_failures", 0) + 1,
        "pending_hashes": pending,
    }


def has_pending_imports(status: dict[str, Any]) -> bool:
    """True якщо є версії merged.csv які Prom ще не отримав."""
    return (
        status.get("status") == "failed"
        and bool(status.get("pending_hashes"))
    )
