"""Persistent run status shared by spiders and the command runner."""

from __future__ import annotations

from pathlib import Path


class RunStatusService:
    """Record a critical spider failure without persisting sensitive details."""

    FAILURE_STATUS = "failure"

    @classmethod
    def clear(cls, project_root: Path, supplier_name: str) -> None:
        cls._status_path(project_root, supplier_name).unlink(missing_ok=True)

    @classmethod
    def mark_failure(cls, project_root: Path, supplier_name: str) -> None:
        path = cls._status_path(project_root, supplier_name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(cls.FAILURE_STATUS, encoding="utf-8")

    @classmethod
    def has_failed(cls, project_root: Path, supplier_name: str) -> bool:
        path = cls._status_path(project_root, supplier_name)
        return path.is_file() and path.read_text(encoding="utf-8").strip() == cls.FAILURE_STATUS

    @staticmethod
    def _status_path(project_root: Path, supplier_name: str) -> Path:
        return project_root / "data" / "output" / f"{supplier_name}_status.txt"
