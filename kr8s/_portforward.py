# SPDX-FileCopyrightText: Copyright (c) 2023-2026, Kr8s Developers (See LICENSE for list)
# SPDX-License-Identifier: BSD 3-Clause License
from __future__ import annotations

import random
import socket
from typing import TYPE_CHECKING, Any, Literal, Union

from httpx2 import websockets

from ._exceptions import ConnectionClosedError

LocalPortType = Union[Literal["match", "auto"], int, None]

if TYPE_CHECKING:
    from ._objects import BaseAPIObject, BasePod

# Give up connecting to the Pod after this many failed attempts
MAX_CONNECTION_ATTEMPTS = 5
# Read up to this many bytes from a TCP connection at a time
TCP_READ_SIZE = 1024 * 1024


class BasePortForward:
    """Start a tcp server and forward all connections to a Pod port.

    Holds the configuration and the port forward protocol. Subclasses run the servers.

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
    """

    def __init__(
        self,
        resource: BaseAPIObject,
        remote_port: int,
        local_port: LocalPortType = "match",
        address: list[str] | str = "127.0.0.1",
    ) -> None:
        from ._objects import BasePod

        self.remote_port = remote_port
        if local_port == "match":
            self.local_port = remote_port
        elif local_port == "auto" or local_port is None:
            self.local_port = 0
        elif isinstance(local_port, int):
            self.local_port = local_port
        else:
            raise TypeError("local_port must be 'match', 'auto', an int or None")
        if isinstance(address, str):
            address = [address]
        self.address = address
        if not isinstance(resource, BasePod) and not hasattr(resource, "ready_pods"):
            raise ValueError(
                "resource must be a Pod or a resource with a ready_pods method"
            )
        self._resource = resource
        self.pod: BasePod | None = None

    @staticmethod
    def _choose_pod(pods: list) -> Any:
        """Choose one of a resource's ready Pods to forward to."""
        try:
            return random.choice(pods)
        except IndexError:
            raise RuntimeError("No ready pods found") from None

    def _websocket_kwargs(self, pod: BasePod) -> dict[str, Any]:
        return {
            "version": pod.version,
            "url": f"{pod.endpoint}/{pod.name}/portforward",
            "namespace": pod.namespace,
            "params": {
                "name": pod.name,
                "namespace": pod.namespace,
                "ports": f"{self.remote_port}",
                "_preload_content": "false",
            },
        }

    @staticmethod
    def _is_permission_error(e: websockets.HTTPXWSException) -> bool:
        return isinstance(
            e, websockets.WebSocketUpgradeError
        ) and e.response.status_code in (401, 403)

    @staticmethod
    def _data_frame(data: bytes) -> bytes:
        """Wrap TCP data in a frame for the websocket."""
        # Send data to channel 0 of the websocket.
        # TODO Support multiple channels for multiple ports.
        return b"\x00" + data

    @staticmethod
    def _frame_data(message: bytes, channels: list[int]) -> bytes | None:
        """Return the TCP data in a websocket frame, or None if there isn't any."""
        # Kubernetes portforward protocol prefixes all frames with a byte to represent
        # the channel. Channel 0 is rw for data and channel 1 is ro for errors.
        if message[0] not in channels:
            # Keep track of our channels. Could be useful later for listening to multiple ports.
            # The first frame on each channel only contains the port number.
            channels.append(message[0])
            return None
        if message[0] % 2 == 1:  # pragma: no cover
            # Odd channels are for errors.
            raise ConnectionClosedError(message[1:].decode())
        return message[1:]

    def _is_port_in_use(self, port: int, host: str = "127.0.0.1") -> bool:
        """Check if a given port is in use on a specified host.

        Args:
            port: Port number to check.
            host: Host address to check the port on. Default is localhost.

        Returns:
            bool: True if the port is in use, False otherwise.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex((host, port)) == 0

    def _find_available_port(self):
        """Find a random port that is not in use on any of the given addresses.

        Returns:
            An available port number.
        """
        while True:
            port = random.randint(10000, 60000)
            if not any(self._is_port_in_use(port, address) for address in self.address):
                return port
