# SPDX-FileCopyrightText: Copyright (c) 2023-2026, Kr8s Developers (See LICENSE for list)
# SPDX-License-Identifier: BSD 3-Clause License
from __future__ import annotations

import base64
import ipaddress
import json
import logging
import os
import pathlib
import ssl
import subprocess
import tempfile
import threading
from typing import Literal

import anyio

from ._async_utils import NamedTemporaryFile, check_output
from ._config import AsyncKubeConfigSet, BaseKubeConfigSet, KubeConfigSet
from ._types import PathType

logger = logging.getLogger(__name__)

# Credentials which may be given either as a file path or as inline data.
# (attribute to set, path key, data key, section of kubeconfig)
_CREDENTIAL_FILES: list[tuple[str, str, str, Literal["user", "cluster"]]] = [
    ("client_key_file", "client-key", "client-key-data", "user"),
    ("client_cert_file", "client-certificate", "client-certificate-data", "user"),
    (
        "server_ca_file",
        "certificate-authority",
        "certificate-authority-data",
        "cluster",
    ),
]


class BaseKubeAuth:
    """Load kubernetes auth from kubeconfig, service account, or url.

    Holds the auth state and every decision that doesn't need I/O.
    Subclasses implement reading files, running exec plugins and writing temporary files.
    """

    kubeconfig: BaseKubeConfigSet

    def __init__(
        self,
        kubeconfig: PathType | None = None,
        url: str | None = None,
        serviceaccount: str | None = None,
        namespace: str | None = None,
        context: str | None = None,
    ) -> None:
        self.server: str = ""
        self.client_cert_file: PathType | None = None
        self.client_key_file: PathType | None = None
        self.server_ca_file: PathType | None = None
        self.token: str | None = None
        self._namespace: str | None = namespace
        self.active_context: str = ""
        self.tls_server_name: str | None = None
        self._url: str = ""
        if url:
            self._url = url
        self._insecure_skip_tls_verify: bool = False
        self._use_context: str | None = context
        self._context: dict = {}
        self._cluster: dict = {}
        self._user: dict = {}
        self._serviceaccount: str = (
            serviceaccount
            if serviceaccount is not None
            else "/var/run/secrets/kubernetes.io/serviceaccount"
        )
        self._kubeconfig_path_or_dict: dict | PathType
        if kubeconfig:
            self._kubeconfig_path_or_dict = kubeconfig
        else:
            self._kubeconfig_path_or_dict = os.environ.get(
                "KUBECONFIG", "~/.kube/config"
            )

    @property
    def namespace(self) -> str:
        return self._namespace if self._namespace else "default"

    @namespace.setter
    def namespace(self, value: str):
        self._namespace = value

    @property
    def proxy(self) -> str | None:
        if "proxy-url" in self._cluster:
            return self._cluster["proxy-url"]
        return None

    def _kubeconfig_sources(self) -> list[PathType | dict]:
        """Return the kubeconfig paths or dicts to load."""
        if isinstance(self._kubeconfig_path_or_dict, (str, pathlib.Path)):
            if os.name != "nt":
                return list(str(self._kubeconfig_path_or_dict).split(":"))
            # Windows doesn't support multiple configs in a path
            return [self._kubeconfig_path_or_dict]
        return [self._kubeconfig_path_or_dict]

    def _ignore_invalid_kubeconfig(self) -> bool:
        """Whether a missing kubeconfig should be skipped rather than raised."""
        return isinstance(self._kubeconfig_path_or_dict, (str, pathlib.Path))

    def _apply_context(self) -> bool:
        """Apply settings from the selected context.

        Returns:
            False if the context has no cluster, which means a service account should be used.
        """
        if self._use_context:
            try:
                self._context = self.kubeconfig.get_context(self._use_context)
                self.active_context = self._use_context
            except ValueError as e:
                raise ValueError(f"No such context {self._use_context}") from e
        elif self.kubeconfig.current_context:
            self._context = self.kubeconfig.get_context(self.kubeconfig.current_context)
            self.active_context = self.kubeconfig.current_context
        else:
            self._context = self.kubeconfig.contexts[0]["context"]
            self.active_context = self.kubeconfig.contexts[0]["name"]

        # Load configuration options from the context
        if self._namespace is None:
            self._namespace = self._context.get("namespace", "default")

        # If no cluster is found in the context, assume it's a service account
        if not self._context["cluster"]:
            return False

        self._cluster = self.kubeconfig.get_cluster(self._context["cluster"])
        self._user = self.kubeconfig.get_user(self._context["user"])
        self.server = self._cluster["server"]

        if (
            "insecure-skip-tls-verify" in self._cluster
            and self._cluster["insecure-skip-tls-verify"]
        ):
            self._insecure_skip_tls_verify = True

        if "tls-server-name" in self._cluster:
            self.tls_server_name = self._cluster["tls-server-name"]
        return True

    def _exec_command(self) -> tuple[list[str], dict[str, str]] | None:
        """Return the exec credential plugin command and environment, if one is configured."""
        if "exec" not in self._user:
            return None
        if self._user["exec"]["apiVersion"] == "client.authentication.k8s.io/v1alpha1":
            raise ValueError(
                "client.authentication.k8s.io/v1alpha1 is not supported for exec auth"
            )
        command = self._user["exec"]["command"]
        args = self._user["exec"].get("args") or []
        env = os.environ.copy()
        env.update(
            **{e["name"]: e["value"] for e in self._user["exec"].get("env") or []}
        )
        return [command, *args], env

    def _apply_exec_credentials(self, output: str) -> None:
        data = json.loads(output)["status"]
        if "token" in data:
            self._user["token"] = data["token"]
        elif "clientCertificateData" in data and "clientKeyData" in data:
            self._user["client-certificate-data"] = data["clientCertificateData"]
            self._user["client-key-data"] = data["clientKeyData"]
        else:
            raise KeyError(
                f"Did not find credentials in {self._user['exec']['command']} output."
            )

    def _credential_files(self):
        """Yield the credential files to resolve, in load order.

        Yields:
            Tuples of ``(attribute, "path", path)`` for credentials given as a path, and
            ``(attribute, "data", bytes)`` for credentials given inline that need writing to a file.
        """
        for attr, path_key, data_key, section in _CREDENTIAL_FILES:
            source = self._user if section == "user" else self._cluster
            if path_key in source:
                yield attr, "path", source[path_key]
            if data_key in source:
                value = source[data_key]
                if "-----" in value:
                    yield attr, "data", value.encode()
                else:
                    yield attr, "data", base64.b64decode(value)

    def _relative_to_kubeconfig(self, path: str) -> pathlib.Path:
        """Resolve a credential path relative to the kubeconfig it came from."""
        return pathlib.Path(self.kubeconfig.get_path(self._use_context)).parent / path

    def _apply_user_settings(self) -> None:
        if "token" in self._user:
            self.token = self._user["token"]
        if "username" in self._user or "password" in self._user:
            raise ValueError(
                "username/password authentication was removed in Kubernetes 1.19, "
                "kr8s doesn't not support this Kubernetes version"
            )
        if "auth-provider" in self._user:
            if p := self._user["auth-provider"]["name"] != "oidc":
                raise ValueError(
                    f"auth-provider {p} was deprecated in Kubernetes 1.21 "
                    "and is not supported by kr8s"
                )
            # TODO: Handle refreshing OIDC token if missing or expired
            self.token = self._user["auth-provider"]["config"]["id-token"]

    def _service_account_server(self) -> str | None:
        """Configure the server for the service account, or return None if there isn't one."""
        self._serviceaccount = os.path.expanduser(self._serviceaccount)
        if not os.path.isdir(self._serviceaccount):
            return None
        self.server = self._format_server_address(
            os.environ.get("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc"),
            os.environ.get("KUBERNETES_SERVICE_PORT", "443"),
        )
        self.server_ca_file = os.path.join(self._serviceaccount, "ca.crt")
        return self.server

    def _set_service_account_token(self, token: str) -> None:
        old_token = self.token
        self.token = token
        if old_token != self.token:
            logger.debug("Found new token")

    def _build_ssl_context(self) -> ssl.SSLContext | bool:
        if (
            not self.client_key_file
            and not self.client_cert_file
            and not self.server_ca_file
            and not self._insecure_skip_tls_verify
        ):
            # If no cert information is provided, fall back to default verification
            return True
        sslcontext = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if self._insecure_skip_tls_verify:
            sslcontext.check_hostname = False
            sslcontext.verify_mode = ssl.CERT_NONE
        if self.client_key_file and self.client_cert_file:
            sslcontext.load_cert_chain(
                certfile=self.client_cert_file,
                keyfile=self.client_key_file,
                password=None,
            )
        if self.server_ca_file:
            sslcontext.load_verify_locations(cafile=self.server_ca_file)
        return sslcontext

    @staticmethod
    def _format_server_address(host, port):
        try:
            ipaddress.IPv6Address(host)
            return f"https://[{host}]:{port}"
        except ipaddress.AddressValueError:
            return f"https://{host}:{port}"


class KubeAuth(BaseKubeAuth):
    """Load kubernetes auth using blocking I/O."""

    kubeconfig: KubeConfigSet

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__auth_lock = threading.Lock()

    def reauthenticate(self) -> None:
        """Reauthenticate with the server."""
        logger.debug("Reloading credentials")
        with self.__auth_lock:
            self.server = ""
            if self._url:
                logger.debug("URL specified manually")
                if self._kubeconfig_path_or_dict is not False:
                    self._load_kubeconfig()
                self.server = self._url
            else:
                if self._kubeconfig_path_or_dict is not False:
                    self._load_kubeconfig()
                if self._serviceaccount and not self.server:
                    self._load_service_account()
            if not self.server:
                raise ValueError("Unable to find valid credentials")

    def ssl_context(self) -> ssl.SSLContext | bool:
        with self.__auth_lock:
            return self._build_ssl_context()

    def _load_kubeconfig(self) -> None:
        """Load kubernetes auth from kubeconfig."""
        logger.debug("Loading kubeconfig")
        try:
            self.kubeconfig = KubeConfigSet(*self._kubeconfig_sources())
        except ValueError:
            if self._ignore_invalid_kubeconfig():
                return
            raise
        if not self._apply_context():
            return
        if exec_command := self._exec_command():
            command, env = exec_command
            result = subprocess.run(command, capture_output=True, env=env, check=False)
            if result.returncode != 0:
                raise RuntimeError(
                    f"Process exited with non-zero code {result.returncode}:\n{result.stderr.decode()}"
                )
            self._apply_exec_credentials(result.stdout.decode())
        for attr, kind, value in self._credential_files():
            if kind == "path":
                if not pathlib.Path(value).exists():
                    value = self._relative_to_kubeconfig(value)
                setattr(self, attr, value)
            else:
                with tempfile.NamedTemporaryFile(delete=False) as fh:
                    fh.write(value)
                setattr(self, attr, fh.name)
        self._apply_user_settings()

    def _load_service_account(self) -> None:
        """Load credentials from service account."""
        logger.debug("Loading service account")
        if not self._service_account_server():
            return
        with open(os.path.join(self._serviceaccount, "token")) as f:
            self._set_service_account_token(f.read())
        # Skip the namespace getter to check if the value is still the default
        if self._namespace is None:
            with open(os.path.join(self._serviceaccount, "namespace")) as f:
                self.namespace = f.read()


class AsyncKubeAuth(BaseKubeAuth):
    """Load kubernetes auth using async I/O."""

    kubeconfig: AsyncKubeConfigSet

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__auth_lock = anyio.Lock()

    def __await__(self):
        async def f():
            await self.reauthenticate()
            return self

        return f().__await__()

    async def reauthenticate(self) -> None:
        """Reauthenticate with the server."""
        logger.debug("Reloading credentials")
        async with self.__auth_lock:
            self.server = ""
            if self._url:
                logger.debug("URL specified manually")
                if self._kubeconfig_path_or_dict is not False:
                    await self._load_kubeconfig()
                self.server = self._url
            else:
                if self._kubeconfig_path_or_dict is not False:
                    await self._load_kubeconfig()
                if self._serviceaccount and not self.server:
                    await self._load_service_account()
            if not self.server:
                raise ValueError("Unable to find valid credentials")

    async def ssl_context(self) -> ssl.SSLContext | bool:
        async with self.__auth_lock:
            return self._build_ssl_context()

    async def _load_kubeconfig(self) -> None:
        """Load kubernetes auth from kubeconfig."""
        logger.debug("Loading kubeconfig")
        try:
            self.kubeconfig = await AsyncKubeConfigSet(*self._kubeconfig_sources())
        except ValueError:
            if self._ignore_invalid_kubeconfig():
                return
            raise
        if not self._apply_context():
            return
        if exec_command := self._exec_command():
            command, env = exec_command
            self._apply_exec_credentials(await check_output(*command, env=env))
        for attr, kind, value in self._credential_files():
            if kind == "path":
                if not await anyio.Path(value).exists():
                    value = self._relative_to_kubeconfig(value)
                setattr(self, attr, value)
            else:
                async with NamedTemporaryFile(delete=False) as fh:
                    await fh.write_bytes(value)
                    setattr(self, attr, str(fh))
        self._apply_user_settings()

    async def _load_service_account(self) -> None:
        """Load credentials from service account."""
        logger.debug("Loading service account")
        if not self._service_account_server():
            return
        async with await anyio.open_file(
            os.path.join(self._serviceaccount, "token")
        ) as f:
            self._set_service_account_token(await f.read())
        # Skip the namespace getter to check if the value is still the default
        if self._namespace is None:
            async with await anyio.open_file(
                os.path.join(self._serviceaccount, "namespace")
            ) as f:
                self.namespace = await f.read()
