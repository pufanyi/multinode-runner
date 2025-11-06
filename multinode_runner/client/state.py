"""Client side state tracking."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class TaskInfo:
    """Cluster wide view of a submitted task."""

    command: str
    created_at: float
    workers: Dict[str, Dict[str, Optional[int]]] = field(default_factory=dict)
    logs: List[Dict[str, str]] = field(default_factory=list)


__all__ = ["TaskInfo"]

