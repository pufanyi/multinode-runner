"""Entry point for the multinode server command."""

from __future__ import annotations

import asyncio
from typing import Optional

from .master import MasterServer
from .worker import WorkerAgent


class ServerApplication:
    """Combined master/worker application that runs on every node."""

    def __init__(self, master_host: str, port: int, bind_host: str = "0.0.0.0") -> None:
        self.master_host = master_host
        self.port = port
        self.bind_host = bind_host
        self.master: Optional[MasterServer] = None

    async def run(self) -> None:
        connect_host = self._normalize_master_host(self.master_host)
        if not await self._probe_master(connect_host, self.port):
            print("[server] master not reachable - starting local coordinator")
            self.master = MasterServer(self.bind_host, self.port)
            await self.master.start()
        agent = WorkerAgent(connect_host, self.port)
        try:
            await agent.run()
        finally:
            if self.master:
                await self.master.close()

    async def _probe_master(self, host: str, port: int) -> bool:
        try:
            fut = asyncio.open_connection(host, port)
            reader, writer = await asyncio.wait_for(fut, timeout=1.5)
            writer.close()
            await writer.wait_closed()
            return True
        except Exception:
            return False

    @staticmethod
    def _normalize_master_host(host: str) -> str:
        if host in {"0.0.0.0", ""}:
            return "127.0.0.1"
        if host in {"localhost"}:
            return "127.0.0.1"
        return host


__all__ = ["ServerApplication"]
