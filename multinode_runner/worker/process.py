"""Process bookkeeping for worker tasks."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass
class RunningProcess:
    """Container storing state for a running subprocess."""

    task_id: str
    command: str
    process: asyncio.subprocess.Process
    stdout_task: asyncio.Task[None]
    stderr_task: asyncio.Task[None]


__all__ = ["RunningProcess"]

