"""Coordinator that manages workers, clients, and tasks."""

from __future__ import annotations

import asyncio
import contextlib
import json
import socket
import time
import uuid

from ..protocol import Message, read_message, send_message
from .state import ClientSession, TaskRecord, WorkerRunState, WorkerSession


class MasterServer:
    """Central coordination server for the multinode runner."""

    def __init__(self, bind_host: str, port: int) -> None:
        self._bind_host = bind_host
        self._port = port
        self._server: asyncio.AbstractServer | None = None
        self._server_task: asyncio.Task[None] | None = None
        self.workers: dict[str, WorkerSession] = {}
        self.clients: dict[str, ClientSession] = {}
        self.tasks: dict[str, TaskRecord] = {}

    async def start(self) -> None:
        """Start accepting connections in the background."""

        if self._server is not None:
            return
        self._server = await asyncio.start_server(
            self._handle_connection, self._bind_host, self._port
        )
        sockets = ", ".join(
            self._describe_socket(sock) for sock in self._server.sockets or []
        )
        print(f"[master] listening on {sockets or 'unknown socket'}")
        self._server_task = asyncio.create_task(self._server.serve_forever())

    async def close(self) -> None:
        """Shut down the listening socket."""

        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        if self._server_task:
            self._server_task.cancel()
        self._server = None
        self._server_task = None

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        address = f"{peer[0]}:{peer[1]}" if peer else "unknown"
        try:
            register = await read_message(reader)
        except (EOFError, ValueError, asyncio.IncompleteReadError) as e:
            print(f"[master] failed to read registration from {address}: {e!r}")
            writer.close()
            await writer.wait_closed()
            return

        role = register.get("role")
        if register.get("type") != "register" or role not in {"worker", "client"}:
            await send_message(
                writer,
                {"type": "error", "message": "first message must be role registration"},
            )
            writer.close()
            await writer.wait_closed()
            return

        if role == "worker":
            worker_id = register.get("worker_id") or str(uuid.uuid4())
            hostname = register.get("hostname", "unknown")
            session = WorkerSession(
                worker_id=worker_id,
                hostname=hostname,
                address=address,
                writer=writer,
                send_queue=asyncio.Queue(),
            )
            await send_message(writer, {"type": "registered", "worker_id": worker_id})
            await self._accept_worker(session, reader)
        else:
            client_id = str(uuid.uuid4())
            session = ClientSession(
                client_id=client_id, writer=writer, send_queue=asyncio.Queue()
            )
            await send_message(writer, {"type": "registered", "client_id": client_id})
            await self._accept_client(session, reader)

    async def _accept_worker(
        self, session: WorkerSession, reader: asyncio.StreamReader
    ) -> None:
        worker_id = session.worker_id
        self.workers[worker_id] = session
        sender = asyncio.create_task(
            self._drain_queue(session.send_queue, session.writer)
        )
        await self._broadcast_cluster()
        print(f"[master] worker {session.hostname} ({worker_id}) connected")
        try:
            while True:
                message = await read_message(reader)
                await self._handle_worker_message(session, message)
        except EOFError:
            print(f"[master] worker {session.hostname} ({worker_id}) disconnected")
        finally:
            sender.cancel()
            self.workers.pop(worker_id, None)
            await self._broadcast_cluster()
            session.writer.close()
            with contextlib.suppress(Exception):
                await session.writer.wait_closed()

    async def _accept_client(
        self, session: ClientSession, reader: asyncio.StreamReader
    ) -> None:
        client_id = session.client_id
        self.clients[client_id] = session
        sender = asyncio.create_task(
            self._drain_queue(session.send_queue, session.writer)
        )
        await self._send_initial_state(session)
        print(f"[master] client {client_id} connected")
        try:
            while True:
                message = await read_message(reader)
                await self._handle_client_message(session, message)
        except EOFError:
            print(f"[master] client {client_id} disconnected")
        finally:
            sender.cancel()
            self.clients.pop(client_id, None)
            session.writer.close()
            with contextlib.suppress(Exception):
                await session.writer.wait_closed()

    async def _handle_worker_message(
        self, session: WorkerSession, message: Message
    ) -> None:
        msg_type = message.get("type")
        if msg_type == "task_started":
            await self._mark_task_running(message["task_id"], session.worker_id)
        elif msg_type == "task_finished":
            await self._mark_task_finished(
                message["task_id"], session.worker_id, message.get("returncode")
            )
        elif msg_type == "task_log":
            await self._handle_task_log(
                message["task_id"],
                session.worker_id,
                message.get("stream", "stdout"),
                message.get("line", ""),
            )
        else:
            print(
                f"[master] unhandled message from worker {session.worker_id}: {json.dumps(message)}"
            )

    async def _handle_client_message(
        self, session: ClientSession, message: Message
    ) -> None:
        msg_type = message.get("type")
        if msg_type == "submit_task":
            command = message.get("command")
            if not isinstance(command, str) or not command.strip():
                await session.send_queue.put(
                    {"type": "error", "message": "command must be a non-empty string"}
                )
                return
            task = await self._create_task(command)
            await session.send_queue.put(
                {"type": "submit_ack", "task_id": task.task_id}
            )
        elif msg_type == "stop_task":
            task_id = message.get("task_id")
            if isinstance(task_id, str) and task_id in self.tasks:
                await self._stop_task(task_id)
            else:
                await session.send_queue.put(
                    {"type": "error", "message": f"unknown task {task_id!r}"}
                )
        elif msg_type == "request_state":
            await self._send_initial_state(session)
        else:
            await session.send_queue.put(
                {"type": "error", "message": f"unknown message type {msg_type!r}"}
            )

    async def _create_task(self, command: str) -> TaskRecord:
        task_id = str(uuid.uuid4())
        task = TaskRecord(
            task_id=task_id,
            command=command,
            created_at=time.time(),
            workers={worker_id: WorkerRunState() for worker_id in self.workers},
        )
        self.tasks[task_id] = task
        await self._broadcast_task_update(task)
        for worker_id, worker in list(self.workers.items()):
            await worker.send_queue.put(
                {"type": "run_task", "task_id": task_id, "command": command}
            )
        print(f"[master] dispatched task {task_id} -> {command}")
        return task

    async def _stop_task(self, task_id: str) -> None:
        task = self.tasks.get(task_id)
        if not task:
            return
        for worker in self.workers.values():
            await worker.send_queue.put({"type": "stop_task", "task_id": task_id})
        print(f"[master] requested stop for task {task_id}")

    async def _mark_task_running(self, task_id: str, worker_id: str) -> None:
        task = self.tasks.get(task_id)
        if not task:
            return
        state = task.workers.setdefault(worker_id, WorkerRunState())
        state.status = "running"
        await self._broadcast_task_update(task)

    async def _mark_task_finished(
        self, task_id: str, worker_id: str, returncode: int | None
    ) -> None:
        task = self.tasks.get(task_id)
        if not task:
            return
        state = task.workers.setdefault(worker_id, WorkerRunState())
        state.status = "finished"
        state.returncode = returncode
        await self._broadcast_task_update(task)

    async def _handle_task_log(
        self, task_id: str, worker_id: str, stream: str, line: str
    ) -> None:
        task = self.tasks.get(task_id)
        entry = {
            "task_id": task_id,
            "worker_id": worker_id,
            "stream": stream,
            "line": line,
            "timestamp": time.time(),
        }
        if task:
            task.workers.setdefault(worker_id, WorkerRunState()).logs.append(entry)
        print(f"[{worker_id} {stream}] {line}")
        await self._broadcast({"type": "task_log", **entry})

    async def _broadcast_task_update(self, task: TaskRecord) -> None:
        payload = {
            "type": "task_update",
            "task_id": task.task_id,
            "command": task.command,
            "created_at": task.created_at,
            "workers": {
                worker_id: {
                    "status": state.status,
                    "returncode": state.returncode,
                }
                for worker_id, state in task.workers.items()
            },
        }
        await self._broadcast(payload)

    async def _broadcast_cluster(self) -> None:
        payload = {
            "type": "cluster_update",
            "workers": [
                {
                    "worker_id": worker.worker_id,
                    "hostname": worker.hostname,
                    "address": worker.address,
                }
                for worker in self.workers.values()
            ],
        }
        await self._broadcast(payload)

    async def _broadcast(self, message: Message) -> None:
        for session in list(self.clients.values()):
            await session.send_queue.put(message)

    async def _send_initial_state(self, session: ClientSession) -> None:
        await session.send_queue.put(
            {
                "type": "cluster_update",
                "workers": [
                    {
                        "worker_id": worker.worker_id,
                        "hostname": worker.hostname,
                        "address": worker.address,
                    }
                    for worker in self.workers.values()
                ],
            }
        )
        for task in self.tasks.values():
            await session.send_queue.put(
                {
                    "type": "task_update",
                    "task_id": task.task_id,
                    "command": task.command,
                    "created_at": task.created_at,
                    "workers": {
                        worker_id: {
                            "status": state.status,
                            "returncode": state.returncode,
                        }
                        for worker_id, state in task.workers.items()
                    },
                }
            )
            for state in task.workers.values():
                for entry in state.logs:
                    await session.send_queue.put({"type": "task_log", **entry})

    async def _drain_queue(
        self, queue: asyncio.Queue[Message], writer: asyncio.StreamWriter
    ) -> None:
        try:
            while True:
                message = await queue.get()
                await send_message(writer, message)
        except asyncio.CancelledError:
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    @staticmethod
    def _describe_socket(sock: socket.socket) -> str:
        try:
            host, port = sock.getsockname()
        except Exception:
            return "?"
        return f"{host}:{port}"


__all__ = ["MasterServer"]
