"""Client side state tracking."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class TaskInfo:
    """Cluster wide view of a submitted task."""

    command: str
    created_at: float
    workers: dict[str, dict[str, int | None]] = field(default_factory=dict)
    logs: list[dict[str, str]] = field(default_factory=list)


__all__ = ["TaskInfo"]
