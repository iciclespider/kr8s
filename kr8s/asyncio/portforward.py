# SPDX-FileCopyrightText: Copyright (c) 2023-2026, Kr8s Developers (See LICENSE for list)
# SPDX-License-Identifier: BSD 3-Clause License
"""Objects for managing a port forward connection.

This module provides a class for managing a port forward connection to a Kubernetes Pod or Service.
"""

# Arguments are documented on the class
# ruff: noqa: D105, D107
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager, suppress
from typing import TYPE_CHECKING

import anyio
import httpx2
from httpx2 import websockets

from kr8s._exceptions import ConnectionClosedError, ServerError
from kr8s._portforward import (
    MAX_CONNECTION_ATTEMPTS,
    TCP_READ_SIZE,
    BasePortForward,
    LocalPortType,
)

if TYPE_CHECKING:
    from kr8s._objects import BaseAPIObject

__all__ = ["PortForward", "LocalPortType"]


class PortForward(BasePortForward):
    """Start a tcp server and forward all connections to a Pod port.

    You can either pass a :class:`kr8s.asyncio.objects.Pod` or any resource with a ``ready_pods`` method
    such as a :class:`kr8s.asyncio.objects.Service`.

    .. note::
        The ``ready_pods`` method should return a list of Pods that are ready to accept connections.

    .. warning:
        Currently Port Forwards only work when using ``asyncio`` and not ``trio``.

    Args:
        resource:
            The Pod or Resource to forward to.
        remote_port:
            The port on the Pod to forward to.
        local_port:
            The local port to listen on. Defaults to ``"match"``, which will match the ``remote_port``.
            Set to ``"auto"`` or ``None`` to find an available high port.
            Set to an ``int`` to specify a specific port.
        address:
            List of addresses or address to listen on. Defaults to ["127.0.0.1"], will listen only on 127.0.0.1.

    Example:
        This class can be used as a an async context manager or with explicit start/stop methods.

        Context manager:

        >>> async with PortForward(pod, 8888) as port:
        ...     print(f"Forwarding to port {port}")
        ...     # Do something with port 8888 on the Pod


        Explict start/stop:

        >>> pf = PortForward(pod, 8888)
        >>> await pf.start()
        >>> print(f"Forwarding to port {pf.local_port}")
        >>> # Do something with port 8888 on the Pod
        >>> await pf.stop()

        Explict bind address:

        >>> async with PortForward(pod, 8888, address=["127.0.0.1", "10.20.0.1"]) as port:
        ...     print(f"Forwarding to port {port}")
        ...     # Do something with port 8888 on the Pod, port will be bind to 127.0.0.1 and 10.20.0.1

    """

    def __init__(
        self,
        resource: BaseAPIObject,
        remote_port: int,
        local_port: LocalPortType = "match",
        address: list[str] | str = "127.0.0.1",
    ) -> None:
        with suppress(ImportError):
            import sniffio

            with suppress(sniffio.AsyncLibraryNotFoundError):
                if sniffio.current_async_library() != "asyncio":
                    raise RuntimeError(
                        "PortForward only works with asyncio, "
                        "see https://github.com/kr8s-org/kr8s/issues/104"
                    )
        super().__init__(resource, remote_port, local_port, address)
        self.server = None
        self.servers: list[asyncio.Server] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tasks: list[asyncio.Task] = []
        self._run_task = None
        self._bg_future: asyncio.Future | None = None
        self._bg_task: asyncio.Task | None = None

    async def __aenter__(self, *args, **kwargs):
        self._run_task = self._run()
        return await self._run_task.__aenter__(*args, **kwargs)

    async def __aexit__(self, *args, **kwargs):
        assert self._run_task
        return await self._run_task.__aexit__(*args, **kwargs)

    async def start(self) -> int:
        """Start a background task with the port forward running."""
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        if self._bg_task is not None:
            return self.local_port

        async def f():
            self._bg_future = self._loop.create_future()
            async with self as port:
                self.local_port = port
                await self._bg_future

        self._bg_task = self._loop.create_task(f())
        while self.local_port == 0:
            await anyio.sleep(0.1)
        return self.local_port

    async def stop(self) -> None:
        """Stop the background task."""
        if self._bg_future:
            self._bg_future.set_result(None)
        self._bg_task = None

    async def run_forever(self) -> None:
        """Run the port forward forever.

        Example:
            >>> pf = pod.portforward(remote_port=8888, local_port=8889)
            >>> # or
            >>> pf = PortForward(pod, remote_port=8888, local_port=8889)
            >>> await pf.run_forever()
        """
        async with self:
            with contextlib.suppress(asyncio.CancelledError):
                for server in self.servers:
                    await server.serve_forever()

    @asynccontextmanager
    async def _run(self) -> AsyncGenerator[int]:
        """Start the port forward for multiple bind addresses and yield the local port."""
        if self.local_port == 0:
            self.local_port = self._find_available_port()

        for address in self.address:
            server = await asyncio.start_server(
                self._sync_sockets, port=self.local_port, host=address
            )
            self.servers.append(server)

        try:
            for server in self.servers:
                await server.start_serving()
            yield self.local_port

        finally:
            # Ensure all servers are closed properly
            for server in self.servers:
                server.close()
                await server.wait_closed()
                self.servers.remove(server)

    async def _select_pod(self):
        """Select a Pod to forward to."""
        from kr8s._objects import BasePod

        if isinstance(self._resource, BasePod):
            return self._resource
        return self._choose_pod(await self._resource.ready_pods())  # type: ignore[attr-defined]

    @asynccontextmanager
    async def _connect_websocket(self):
        """Connect to the Kubernetes portforward websocket."""
        connection_attempts = 0
        while True:
            if not self.pod:
                self.pod = await self._select_pod()
            try:
                assert self.pod.api
                async with self.pod.api.open_websocket(
                    **self._websocket_kwargs(self.pod)
                ) as websocket:
                    yield websocket
                    break
            except websockets.HTTPXWSException as e:
                self.pod = None
                connection_attempts += 1
                if connection_attempts > MAX_CONNECTION_ATTEMPTS:
                    if self._is_permission_error(e):
                        assert isinstance(e, websockets.WebSocketUpgradeError)
                        error_message = f"Permission denied: {e.response.status_code}"
                        with suppress(httpx2.StreamClosed, httpx2.ResponseNotRead):
                            await e.response.aread()
                            error_message = e.response.text
                        raise ServerError(error_message, response=e.response) from e
                    raise ConnectionClosedError("Unable to connect to Pod") from e
                await anyio.sleep(0.1 * connection_attempts)

    async def _sync_sockets(self, reader, writer) -> None:
        """Start two tasks to copy bytes from tcp=>websocket and websocket=>tcp."""
        try:
            async with self._connect_websocket() as ws:
                with suppress(ConnectionClosedError, websockets.WebSocketDisconnect):
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(self._tcp_to_ws, ws, reader)
                        tg.start_soon(self._ws_to_tcp, ws, writer)
        finally:
            writer.close()

    async def _tcp_to_ws(self, ws, reader) -> None:
        while True:
            try:
                data = await reader.read(TCP_READ_SIZE)
            except ConnectionResetError:
                data = None
            if not data:
                raise ConnectionClosedError("TCP socket closed")
            try:
                await ws.send_bytes(self._data_frame(data))
            except ConnectionResetError as e:
                raise ConnectionClosedError("Websocket closed") from e

    async def _ws_to_tcp(self, ws, writer) -> None:
        channels: list[int] = []
        while True:
            data = self._frame_data(await ws.receive_bytes(), channels)
            if data is not None:
                writer.write(data)
                await writer.drain()
