"""Utility helpers for the newline delimited JSON control protocol."""

from __future__ import annotations

import asyncio
import json
from typing import Any

Message = dict[str, Any]


def encode_message(message: Message) -> bytes:
    """Serialize *message* to a UTF-8 encoded JSON line."""

    return (json.dumps(message, separators=(",", ":")) + "\n").encode("utf-8")


async def send_message(writer: asyncio.StreamWriter, message: Message) -> None:
    """Send a JSON *message* to *writer*."""

    writer.write(encode_message(message))
    await writer.drain()


async def read_message(reader: asyncio.StreamReader) -> Message:
    """Read a JSON message from *reader*.

    Raises
    ------
    EOFError
        If the stream is closed while waiting for a new message.
    ValueError
        If the incoming payload is not valid JSON.
    """

    line = await reader.readline()
    if not line:
        raise EOFError("connection closed")
    return json.loads(line.decode("utf-8"))
