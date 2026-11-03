# SPDX-FileCopyrightText: Copyright (c) 2023-2026, Kr8s Developers (See LICENSE for list)
# SPDX-License-Identifier: BSD 3-Clause License
from __future__ import annotations

import json
from collections.abc import AsyncGenerator, Generator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, BinaryIO

from kr8s._exceptions import ExecError

if TYPE_CHECKING:
    from kr8s._objects import BasePod

STDIN_CHANNEL: int = 0
STDOUT_CHANNEL: int = 1
STDERR_CHANNEL: int = 2
ERROR_CHANNEL: int = 3
RESIZE_CHANNEL: int = 4
CLOSE_CHANNEL: int = 255
EXEC_PROTOCOL: str = "v4.channel.k8s.io"


class BaseExec:
    """Executes a command in a running container.

    Builds the request and decodes the channel protocol. Subclasses talk to the websocket.
    """

    def __init__(
        self,
        resource: BasePod,
        command: list[str],
        container: str | None = None,
        stdin: str | BinaryIO | None = None,
        stdout: BinaryIO | None = None,
        stderr: BinaryIO | None = None,
        check: bool = True,
        capture_output: bool = True,
        timeout: int | None = None,
    ) -> None:
        self._resource = resource
        self._container = container

        self._stdin = stdin
        self._stdout = stdout
        self._stderr = stderr
        self._capture_output = capture_output
        self._timeout = timeout

        self.args = command
        self.stdout = b""
        self.stderr = b""
        self.returncode: int
        self.check = check

    def _websocket_kwargs(self) -> dict[str, Any]:
        return {
            "version": self._resource.version,
            "url": f"{self._resource.endpoint}/{self._resource.name}/exec",
            "subprotocols": (EXEC_PROTOCOL,),
            "namespace": self._resource.namespace,
            "params": {
                "command": self.args,
                "container": self._container or self._resource.spec.containers[0].name,
                "stdout": "true" if self._stdout or self._capture_output else "false",
                "stderr": "true" if self._stderr or self._capture_output else "false",
                "stdin": "true" if self._stdin is not None else "false",
            },
        }

    def _stdin_frames(self, subprotocol: str | None) -> list[bytes]:
        """Return the frames that send stdin and then close it."""
        if not self._stdin:
            return []
        if subprotocol != "v5.channel.k8s.io":
            raise ExecError(
                "Stdin is not supported with protocol "
                f"{subprotocol}, only with v5.channel.k8s.io"
            )
        if isinstance(self._stdin, str):
            data = self._stdin.encode()
        else:
            data = self._stdin.read()
        return [
            STDIN_CHANNEL.to_bytes() + data,
            CLOSE_CHANNEL.to_bytes() + STDIN_CHANNEL.to_bytes(),
        ]

    def _handle_message(self, message: bytes) -> bool:
        """Handle a message from the websocket and return whether the command has finished."""
        channel, message = int(message[0]), message[1:]
        if not message:
            return False
        if channel == STDOUT_CHANNEL:
            if self._capture_output:
                self.stdout += message
            if self._stdout:
                self._stdout.write(message)
        elif channel == STDERR_CHANNEL:
            if self._capture_output:
                self.stderr += message
            if self._stderr:
                self._stderr.write(message)
        elif channel == ERROR_CHANNEL:
            error = json.loads(message.decode())
            if error["status"] == "Success":
                self.returncode = 0
                return True
            # Extract return code from details
            if "details" in error and "causes" in error["details"]:
                for cause in error["details"]["causes"]:
                    if "reason" in cause and cause["reason"] == "ExitCode":
                        self.returncode = int(cause["message"])
                        break
            else:
                self.returncode = 1
            if self.check:
                raise ExecError(error["message"])
            return True
        else:
            raise ExecError(f"Unhandled message on channel {channel}: {message!r}")
        return False

    def as_completed(self) -> CompletedExec:
        return CompletedExec(
            args=self.args,
            stdout=self.stdout,
            stderr=self.stderr,
            returncode=self.returncode,
        )


class Exec(BaseExec):
    """Executes a command in a running container using blocking I/O."""

    @contextmanager
    def run(self) -> Generator[Exec]:
        assert self._resource.api
        with self._resource.api.open_websocket(**self._websocket_kwargs()) as ws:
            for frame in self._stdin_frames(ws.subprotocol):
                ws.send_bytes(frame)
            while not self._handle_message(ws.receive_bytes(timeout=self._timeout)):
                pass
            yield self

    def wait(self) -> CompletedExec:
        return self.as_completed()


class AsyncExec(BaseExec):
    """Executes a command in a running container using async I/O."""

    @asynccontextmanager
    async def run(self) -> AsyncGenerator[AsyncExec]:
        assert self._resource.api
        async with self._resource.api.open_websocket(**self._websocket_kwargs()) as ws:
            for frame in self._stdin_frames(ws.subprotocol):
                await ws.send_bytes(frame)
            while not self._handle_message(
                await ws.receive_bytes(timeout=self._timeout)
            ):
                pass
            yield self

    async def wait(self) -> CompletedExec:
        return self.as_completed()


@dataclass
class CompletedExec:
    """Result of an Exec command.

    Similar to subprocess.CompletedProcess.
    """

    args: str | list[str]
    stdout: bytes
    stderr: bytes
    returncode: int

    def check_returncode(self) -> None:
        if self.returncode != 0:
            raise ExecError(f"Command {self.args} exited with status {self.returncode}")
