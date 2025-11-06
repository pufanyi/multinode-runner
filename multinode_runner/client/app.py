"""Interactive client used to control the cluster from the terminal."""

from __future__ import annotations

import asyncio
import contextlib
import json
import sys
import textwrap
import time
from typing import Dict, Optional

from ..protocol import read_message, send_message
from .state import TaskInfo


class ClientApplication:
    """Simple text based user interface for cluster management."""

    def __init__(self, master_host: str, port: int) -> None:
        self.master_host = master_host
        self.port = port
        self.tasks: Dict[str, TaskInfo] = {}
        self.workers: Dict[str, Dict[str, str]] = {}
        self._receive_task: Optional[asyncio.Task[None]] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    async def run(self) -> None:
        host = self._normalize_host(self.master_host)
        reader, writer = await asyncio.open_connection(host, self.port)
        await send_message(writer, {"type": "register", "role": "client"})
        ack = await read_message(reader)
        if ack.get("type") != "registered":
            raise RuntimeError("registration rejected by master")
        self._writer = writer
        self._receive_task = asyncio.create_task(self._receiver_loop(reader))
        print(
            textwrap.dedent(
                """
                Connected to cluster. Available commands:
                  submit <command>   Submit a shell command to run on every worker
                  stop <task_id>     Stop the task on every worker
                  tasks              List submitted tasks and their status
                  workers            Show connected workers
                  logs <task_id>     Display the collected logs for a task
                  help               Show this message again
                  quit               Exit the client
                """
            )
        )
        try:
            await self._input_loop()
        finally:
            if self._receive_task:
                self._receive_task.cancel()
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _input_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            command = await loop.run_in_executor(None, lambda: input("client> ").strip())
            if not command:
                continue
            if command in {"quit", "exit"}:
                break
            if command == "help":
                print("Type one of: submit, stop, tasks, workers, logs, quit")
                continue
            if command == "tasks":
                self._print_tasks()
                continue
            if command == "workers":
                self._print_workers()
                continue
            if command.startswith("submit "):
                await self._submit(command[len("submit ") :].strip())
                continue
            if command.startswith("stop "):
                await self._stop(command[len("stop ") :].strip())
                continue
            if command.startswith("logs "):
                self._show_logs(command[len("logs ") :].strip())
                continue
            print("Unknown command. Type 'help' for usage.")

    async def _submit(self, command: str) -> None:
        if not command:
            print("Command cannot be empty")
            return
        await self._send({"type": "submit_task", "command": command})

    async def _stop(self, task_id: str) -> None:
        if not task_id:
            print("Task id required")
            return
        await self._send({"type": "stop_task", "task_id": task_id})

    async def _send(self, message: Dict[str, object]) -> None:
        if not self._writer:
            return
        await send_message(self._writer, message)

    async def _receiver_loop(self, reader: asyncio.StreamReader) -> None:
        try:
            while True:
                message = await read_message(reader)
                await self._handle_message(message)
        except EOFError:
            print("Connection to master lost", file=sys.stderr)
        finally:
            self._writer = None

    async def _handle_message(self, message: Dict[str, object]) -> None:
        msg_type = message.get("type")
        if msg_type == "cluster_update":
            self.workers = {entry["worker_id"]: entry for entry in message.get("workers", [])}
            print(f"Updated worker list ({len(self.workers)} online)")
        elif msg_type == "task_update":
            task_id = message["task_id"]
            info = self.tasks.setdefault(
                task_id,
                TaskInfo(command=message.get("command", ""), created_at=float(message.get("created_at", time.time()))),
            )
            info.command = message.get("command", info.command)
            info.created_at = float(message.get("created_at", info.created_at))
            info.workers.update(message.get("workers", {}))
            print(f"Task {task_id} updated")
        elif msg_type == "task_log":
            task_id = message.get("task_id")
            info = self.tasks.setdefault(task_id, TaskInfo(command="<unknown>", created_at=time.time()))
            info.logs.append(message)
            worker = message.get("worker_id", "?")
            stream = message.get("stream", "stdout")
            line = message.get("line", "")
            print(f"[{task_id} {worker} {stream}] {line}")
        elif msg_type == "submit_ack":
            print(f"Task submitted: {message.get('task_id')}")
        elif msg_type == "error":
            print(f"Error from server: {message.get('message')}", file=sys.stderr)
        else:
            print(f"Unknown message: {json.dumps(message)}")

    def _print_workers(self) -> None:
        if not self.workers:
            print("No workers connected")
            return
        print("Workers:")
        for worker in self.workers.values():
            print(f"  {worker['worker_id']} ({worker.get('hostname', '?')}) @ {worker.get('address', '?')}")

    def _print_tasks(self) -> None:
        if not self.tasks:
            print("No tasks submitted")
            return
        print("Tasks:")
        for task_id, info in self.tasks.items():
            created = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(info.created_at))
            print(f"  {task_id} -> {info.command} (submitted {created})")
            for worker_id, state in info.workers.items():
                status = state.get("status", "unknown")
                returncode = state.get("returncode")
                extra = f" rc={returncode}" if returncode is not None else ""
                print(f"    - {worker_id}: {status}{extra}")

    def _show_logs(self, task_id: str) -> None:
        info = self.tasks.get(task_id)
        if not info:
            print(f"Unknown task {task_id}")
            return
        if not info.logs:
            print("No logs yet")
            return
        print(f"Logs for task {task_id}:")
        for entry in info.logs:
            worker = entry.get("worker_id", "?")
            stream = entry.get("stream", "stdout")
            line = entry.get("line", "")
            print(f"  [{worker} {stream}] {line}")

    @staticmethod
    def _normalize_host(host: str) -> str:
        if host in {"", "0.0.0.0", "localhost"}:
            return "127.0.0.1"
        return host


__all__ = ["ClientApplication"]

