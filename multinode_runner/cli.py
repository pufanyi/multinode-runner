"""Command line interface for multinode-runner."""

from __future__ import annotations

import argparse
import asyncio
from typing import Sequence

from .client import ClientApplication
from .server import ServerApplication


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="multinode-runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    server_parser = subparsers.add_parser("server", help="Start the node server")
    server_parser.add_argument("--master", required=True, help="Master host to connect to")
    server_parser.add_argument("--port", type=int, default=9000, help="Port to connect/listen on")
    server_parser.add_argument("--bind", default="0.0.0.0", help="Bind host for the master server when elected")

    client_parser = subparsers.add_parser("client", help="Start the interactive client")
    client_parser.add_argument("--master", required=True, help="Master host to connect to")
    client_parser.add_argument("--port", type=int, default=9000, help="Port to connect to")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "server":
        app = ServerApplication(args.master, args.port, bind_host=args.bind)
    else:
        app = ClientApplication(args.master, args.port)

    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:  # pragma: no cover - manual interruption
        return 130
    return 0


__all__ = ["build_parser", "main"]
