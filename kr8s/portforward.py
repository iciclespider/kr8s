# SPDX-FileCopyrightText: Copyright (c) 2024-2026, Kr8s Developers (See LICENSE for list)
# SPDX-License-Identifier: BSD 3-Clause License
"""Objects for managing a port forward connection.

This module provides a class for managing a port forward connection to a Kubernetes Pod or Service.
"""

# Arguments are documented on the class
# ruff: noqa: D105, D107
from __future__ import annotations

import socket
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager, suppress

import httpx2
from httpx2 import websockets

from ._exceptions import ConnectionClosedError, ServerError
from ._portforward import (
    MAX_CONNECTION_ATTEMPTS,
    TCP_READ_SIZE,
    BasePortForward,
    LocalPortType,
)

__all__ = ["PortForward", "LocalPortType"]

# How often blocked threads wake up to check whether the port forward has stopped
POLL_INTERVAL = 0.1


class PortForward(BasePortForward):
    """Start a tcp server and forward all connections to a Pod port.

    You can either pass a :class:`kr8s.objects.Pod` or any resource with a ``ready_pods`` method
    such as a :class:`kr8s.objects.Service`.

    Each bind address is served by a background thread, and each connection uses two more threads
    to copy data between the TCP socket and the Kubernetes websocket.

    .. note::
        The ``ready_pods`` method should return a list of Pods that are ready to accept connections.

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
        This class can be used as a context manager or with explicit start/stop methods.

        Context manager:

        >>> with PortForward(pod, 8888) as port:
        ...     print(f"Forwarding to port {port}")
        ...     # Do something with port 8888 on the Pod


        Explict start/stop:

        >>> pf = PortForward(pod, 8888)
        >>> pf.start()
        >>> print(f"Forwarding to port {pf.local_port}")
        >>> # Do something with port 8888 on the Pod
        >>> pf.stop()

    """

    def __init__(
        self,
        resource,
        remote_port: int,
        local_port: LocalPortType = "match",
        address: list[str] | str = "127.0.0.1",
    ) -> None:
        super().__init__(resource, remote_port, local_port, address)
        self.servers: list[socket.socket] = []
        self._serve_threads: list[threading.Thread] = []
        self._stopped = threading.Event()
        self._pod_lock = threading.Lock()

    def __enter__(self) -> int:
        return self.start()

    def __exit__(self, *args) -> None:
        self.stop()

    def start(self) -> int:
        """Start serving the port forward in background threads.

        Returns:
            The local port.
        """
        if self.servers:
            return self.local_port
        if self.local_port == 0:
            self.local_port = self._find_available_port()
        self._stopped.clear()
        for address in self.address:
            family = socket.AF_INET6 if ":" in address else socket.AF_INET
            server = socket.create_server((address, self.local_port), family=family)
            server.settimeout(POLL_INTERVAL)
            self.servers.append(server)
            thread = threading.Thread(
                target=self._serve, args=(server,), daemon=True, name="kr8s-portforward"
            )
            thread.start()
            self._serve_threads.append(thread)
        return self.local_port

    def stop(self) -> None:
        """Stop the port forward."""
        self._stopped.set()
        # Wait for the threads to stop accepting before closing the servers. On Linux a
        # socket closed while another thread waits in accept() keeps queueing connections
        # until that wait returns, and then resets them.
        for thread in self._serve_threads:
            if thread is not threading.current_thread():
                thread.join()
        self._serve_threads.clear()
        for server in self.servers:
            server.close()
        self.servers.clear()

    def run_forever(self) -> None:
        """Run the port forward until :meth:`stop` is called.

        Example:
            >>> pf = pod.portforward(remote_port=8888, local_port=8889)
            >>> # or
            >>> pf = PortForward(pod, remote_port=8888, local_port=8889)
            >>> pf.run_forever()
        """
        with self:
            self._stopped.wait()

    def _serve(self, server: socket.socket) -> None:
        """Accept connections until the port forward is stopped."""
        while not self._stopped.is_set():
            try:
                conn, _ = server.accept()
            except TimeoutError:
                continue
            except OSError:
                # The server socket failed, so stop serving on it
                return
            conn.settimeout(None)
            threading.Thread(
                target=self._sync_sockets,
                args=(conn,),
                daemon=True,
                name="kr8s-portforward-connection",
            ).start()

    def _select_pod(self):
        """Select a Pod to forward to."""
        from ._objects import BasePod

        if isinstance(self._resource, BasePod):
            return self._resource
        return self._choose_pod(self._resource.ready_pods())  # type: ignore[attr-defined]

    @contextmanager
    def _connect_websocket(self) -> Generator[websockets.WebSocketSession]:
        """Connect to the Kubernetes portforward websocket."""
        connection_attempts = 0
        while True:
            with self._pod_lock:
                if not self.pod:
                    self.pod = self._select_pod()
                pod = self.pod
            try:
                assert pod.api
                with pod.api.open_websocket(**self._websocket_kwargs(pod)) as websocket:
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
                            e.response.read()
                            error_message = e.response.text
                        raise ServerError(error_message, response=e.response) from e
                    raise ConnectionClosedError("Unable to connect to Pod") from e
                time.sleep(0.1 * connection_attempts)

    def _sync_sockets(self, conn: socket.socket) -> None:
        """Copy bytes from tcp=>websocket and websocket=>tcp until either side closes."""
        with conn, suppress(ConnectionClosedError, websockets.WebSocketDisconnect):
            with self._connect_websocket() as ws:
                done = threading.Event()
                reader = threading.Thread(
                    target=self._ws_to_tcp, args=(ws, conn, done), daemon=True
                )
                reader.start()
                try:
                    self._tcp_to_ws(ws, conn, done)
                finally:
                    done.set()
                    reader.join()

    def _tcp_to_ws(
        self,
        ws: websockets.WebSocketSession,
        conn: socket.socket,
        done: threading.Event,
    ) -> None:
        while not done.is_set():
            try:
                data = conn.recv(TCP_READ_SIZE)
            except OSError:
                data = b""
            if not data:
                return
            try:
                ws.send_bytes(self._data_frame(data))
            except ConnectionResetError as e:
                raise ConnectionClosedError("Websocket closed") from e

    def _ws_to_tcp(
        self,
        ws: websockets.WebSocketSession,
        conn: socket.socket,
        done: threading.Event,
    ) -> None:
        channels: list[int] = []
        try:
            while not done.is_set() and not self._stopped.is_set():
                try:
                    message = ws.receive_bytes(timeout=POLL_INTERVAL)
                except TimeoutError:
                    continue
                data = self._frame_data(message, channels)
                if data:
                    conn.sendall(data)
        except (ConnectionClosedError, websockets.WebSocketDisconnect, OSError):
            pass
        finally:
            done.set()
            # Wake up the thread blocked reading from the TCP socket
            with suppress(OSError):
                conn.shutdown(socket.SHUT_RDWR)
