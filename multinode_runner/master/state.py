"""Data models used by the master server."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ..protocol import Message


@dataclass
class WorkerRunState:
    """Mutable state for a single worker execution."""

    status: str = "pending"
    returncode: Optional[int] = None
    logs: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class TaskRecord:
    """Representation of a task running on the cluster."""

    task_id: str
    command: str
    created_at: float
    workers: Dict[str, WorkerRunState]


@dataclass
class WorkerSession:
    """Book-keeping for an attached worker."""

    worker_id: str
    hostname: str
    address: str
    writer: asyncio.StreamWriter
    send_queue: "asyncio.Queue[Message]"


@dataclass
class ClientSession:
    """Representation of a connected client."""

    client_id: str
    writer: asyncio.StreamWriter
    send_queue: "asyncio.Queue[Message]"


__all__ = [
    "WorkerRunState",
    "TaskRecord",
    "WorkerSession",
    "ClientSession",
]

