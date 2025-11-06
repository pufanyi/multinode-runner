"""Worker agent that connects to the master server and executes commands."""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import socket
import sys
import uuid
from typing import Dict, Optional

from ..protocol import read_message, send_message
from .process import RunningProcess


class WorkerAgent:
    """Agent responsible for connecting to the master and running tasks."""

    def __init__(self, master_host: str, master_port: int, reconnect_delay: float = 3.0) -> None:
        self.master_host = master_host
        self.master_port = master_port
        self.reconnect_delay = reconnect_delay
        self.worker_id = str(uuid.uuid4())
        self.processes: Dict[str, RunningProcess] = {}
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        """Attempt to keep a connection to the master alive indefinitely."""

        while not self._stop_event.is_set():
            try:
                await self._connect_and_run()
            except asyncio.CancelledError:  # pragma: no cover - shutdown path
                break
            except Exception as exc:  # pragma: no cover - best-effort logging
                print(f"[worker] connection error: {exc}", file=sys.stderr)
                await asyncio.sleep(self.reconnect_delay)

    async def _connect_and_run(self) -> None:
        reader, writer = await asyncio.open_connection(self.master_host, self.master_port)
        hostname = socket.gethostname()
        register = {"type": "register", "role": "worker", "worker_id": self.worker_id, "hostname": hostname}
        await send_message(writer, register)
        ack = await read_message(reader)
        if ack.get("type") != "registered":
            raise RuntimeError("registration rejected by master")
        print(f"[worker] connected to master at {self.master_host}:{self.master_port} as {self.worker_id}")
        receiver = asyncio.create_task(self._receiver_loop(reader, writer))
        try:
            await receiver
        finally:
            receiver.cancel()
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _receiver_loop(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while True:
            message = await read_message(reader)
            msg_type = message.get("type")
            if msg_type == "run_task":
                await self._start_task(message["task_id"], message.get("command", ""), writer)
            elif msg_type == "stop_task":
                await self._stop_task(message.get("task_id"))
            else:
                print(f"[worker] unhandled message from master: {message}")

    async def _start_task(self, task_id: str, command: str, writer: asyncio.StreamWriter) -> None:
        if not command:
            return
        if task_id in self.processes:
            print(f"[worker] task {task_id} already running")
            return
        print(f"[worker] starting task {task_id}: {command}")
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=os.environ.copy(),
        )
        stdout_task = asyncio.create_task(self._forward_stream(task_id, "stdout", process.stdout, writer))
        stderr_task = asyncio.create_task(self._forward_stream(task_id, "stderr", process.stderr, writer))
        self.processes[task_id] = RunningProcess(task_id, command, process, stdout_task, stderr_task)
        await send_message(writer, {"type": "task_started", "task_id": task_id})
        asyncio.create_task(self._wait_for_completion(task_id, process, writer))

    async def _forward_stream(
        self,
        task_id: str,
        stream_name: str,
        stream: Optional[asyncio.StreamReader],
        writer: asyncio.StreamWriter,
    ) -> None:
        if stream is None:
            return
        while True:
            line = await stream.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").rstrip("\n")
            print(f"[{stream_name}] {text}")
            await send_message(
                writer,
                {
                    "type": "task_log",
                    "task_id": task_id,
                    "stream": stream_name,
                    "line": text,
                },
            )

    async def _wait_for_completion(
        self, task_id: str, process: asyncio.subprocess.Process, writer: asyncio.StreamWriter
    ) -> None:
        returncode = await process.wait()
        print(f"[worker] task {task_id} finished with code {returncode}")
        await send_message(writer, {"type": "task_finished", "task_id": task_id, "returncode": returncode})
        running = self.processes.pop(task_id, None)
        if running:
            running.stdout_task.cancel()
            running.stderr_task.cancel()

    async def _stop_task(self, task_id: Optional[str]) -> None:
        if not task_id:
            return
        process = self.processes.get(task_id)
        if not process:
            return
        print(f"[worker] stopping task {task_id}")
        try:
            process.process.send_signal(signal.SIGINT)
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(process.process.wait(), timeout=5)
        except asyncio.TimeoutError:
            process.process.kill()
            await process.process.wait()
        process.stdout_task.cancel()
        process.stderr_task.cancel()
        self.processes.pop(task_id, None)

    async def stop(self) -> None:
        """Request the agent to shut down."""

        self._stop_event.set()


__all__ = ["WorkerAgent"]

