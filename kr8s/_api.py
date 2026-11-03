# SPDX-FileCopyrightText: Copyright (c) 2023-2026, Kr8s Developers (See LICENSE for list)
# SPDX-License-Identifier: BSD 3-Clause License
from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import logging
import ssl
import threading
import time
import warnings
import weakref
from collections.abc import AsyncGenerator, Generator
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    TypeVar,
)

import anyio
import httpx2
from cryptography import x509
from httpx2 import websockets
from packaging.version import InvalidVersion
from packaging.version import parse as parse_version

from ._auth import AsyncKubeAuth, BaseKubeAuth, KubeAuth
from ._constants import (
    KUBERNETES_MAXIMUM_SUPPORTED_VERSION,
    KUBERNETES_MINIMUM_SUPPORTED_VERSION,
)
from ._data_utils import dict_to_selector, sort_versions
from ._exceptions import APITimeoutError, ServerError
from ._version import __version__

if TYPE_CHECKING:
    from ._objects import BaseAPIObject

ALL = "all"
logger = logging.getLogger(__name__)

# Cache API resources for 6 hours because kubectl does
# https://github.com/kubernetes/cli-runtime/blob/980bedf450ab21617b33d68331786942227fe93a/pkg/genericclioptions/config_flags.go#L297
API_RESOURCES_CACHE_TTL = 60 * 60 * 6

# Retry SSL verification and auth failures this many times before giving up
MAX_RETRIES = 3


@contextmanager
def _httpx_ws_exception_fixer():
    try:
        yield
    except BaseException as exc:
        while isinstance(exc, BaseExceptionGroup) and len(exc.exceptions) == 1:
            exc = exc.exceptions[0]
        raise exc


def _thread_loop_id() -> str:
    """Identify the current thread and event loop, for caching Api instances."""
    thread_id = threading.get_ident()
    try:
        loop_id = id(asyncio.get_running_loop())
    except RuntimeError:
        loop_id = 0
    return f"{thread_id}.{loop_id}"


AuthT = TypeVar("AuthT", bound=BaseKubeAuth)
SessionT = TypeVar("SessionT", httpx2.Client, httpx2.AsyncClient)


class BaseApi(Generic[AuthT, SessionT]):
    """Shared state and logic for the sync and async Kubernetes API clients.

    Nothing here performs I/O. :class:`Api` and :class:`AsyncApi` implement the requests.

    .. warning::
        This class is not intended to be instantiated directly. Instead, use the
        :func:`kr8s.api` function to get a singleton instance of the API.

        See https://docs.kr8s.org/en/stable/client.html#client-caching.

    """

    _asyncio: bool
    _auth_cls: type[AuthT]
    # Each subclass declares its own cache so sync and async clients are never mixed up
    _instances: dict[str, weakref.WeakValueDictionary]

    def __init__(self, **kwargs) -> None:
        if not kwargs.pop("bypass_factory", False):
            raise ValueError(
                "Use kr8s.api() to get an instance of Api. "
                "See https://docs.kr8s.org/en/stable/client.html#client-caching."
            )

        self._url = kwargs.get("url")
        self._kubeconfig = kwargs.get("kubeconfig")
        self._serviceaccount = kwargs.get("serviceaccount")
        self._session: SessionT | None = None
        self._timeout = None
        self._api_resources: list[dict] | None = None
        self._api_resources_expiry = 0.0
        self.auth: AuthT = self._auth_cls(
            url=self._url,
            kubeconfig=self._kubeconfig,
            serviceaccount=self._serviceaccount,
            namespace=kwargs.get("namespace"),
            context=kwargs.get("context"),
        )
        thread_loop_id = _thread_loop_id()
        if thread_loop_id not in self._instances:
            self._instances[thread_loop_id] = weakref.WeakValueDictionary()
        key = hash_kwargs(kwargs)
        self._instances[thread_loop_id][key] = self

    @classmethod
    def _cached_instance(cls, **kwargs) -> BaseApi | None:
        """Return a cached instance for these arguments in this thread and event loop."""
        key = hash_kwargs(kwargs)
        instances = cls._instances.get(_thread_loop_id())
        if not instances:
            return None
        if key in instances:
            return instances[key]
        if all(k is None for k in kwargs.values()) and list(instances.values()):
            return list(instances.values())[0]
        return None

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        self._timeout = value
        if self._session:
            self._session.timeout = value

    def _session_kwargs(self, verify: ssl.SSLContext | bool) -> dict[str, Any]:
        headers = {"User-Agent": self.__version__, "content-type": "application/json"}
        if self.auth.token:
            headers["Authorization"] = f"Bearer {self.auth.token}"
        return {
            "base_url": self.auth.server,
            "headers": headers,
            "verify": verify,
            "timeout": self._timeout,
            "follow_redirects": True,
            "proxy": self.auth.proxy,
        }

    def _construct_url(
        self,
        version: str = "v1",
        base: str = "",
        namespace: str | None = None,
        url: str = "",
    ) -> str:
        if not base:
            if version == "v1":
                base = "/api"
            elif "/" in version:
                base = "/apis"
            else:
                raise ValueError("Unknown API version, base must be specified.")
        parts = [base]
        if version:
            parts.append(version)
        if namespace:
            parts.extend(["namespaces", namespace])
        parts.append(url)
        return "/".join(parts)

    def _request_kwargs(
        self,
        version: str,
        base: str,
        namespace: str | None,
        url: str,
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        kwargs.update(url=self._construct_url(version, base, namespace, url))
        if self.auth.tls_server_name:
            kwargs["extensions"] = {"sni_hostname": self.auth.tls_server_name}
        return kwargs

    @staticmethod
    def _should_reauthenticate(e: httpx2.HTTPStatusError, attempts: int) -> bool:
        # If we get a 401 or 403 our credentials may have expired so we
        # reauthenticate and try again a few times before giving up.
        if e.response.status_code in (401, 403) and attempts < MAX_RETRIES:
            logger.debug(
                f"Unauthorized {e.response.status_code} error, reauthenticating attempt {attempts}"
            )
            return True
        return False

    @staticmethod
    def _server_error(e: httpx2.HTTPStatusError) -> ServerError | None:
        """Convert an HTTP error to a ServerError, or None to re-raise it unchanged."""
        if e.response.status_code >= 400 and e.response.status_code < 500:
            try:
                error = e.response.json()
                error_message = error["message"]
            except json.JSONDecodeError:
                error = e.response.text
                error_message = str(e)
            return ServerError(error_message, status=error, response=e.response)
        elif e.response.status_code >= 500:
            return ServerError(
                str(e),
                status=str(e.response.status_code),
                response=e.response,
            )
        return None

    @staticmethod
    def _should_retry_websocket(
        e: websockets.WebSocketDisconnect, attempts: int
    ) -> bool | None:
        """Decide what to do when a websocket disconnects.

        Returns:
            True to reauthenticate and retry, False to re-raise, None if it was a clean close.
        """
        if e.code and e.code != 1000:
            return e.code in (401, 403) and attempts < MAX_RETRIES
        return None

    def _warn_if_unsupported(self, version: dict) -> None:
        git_version = version["gitVersion"]

        supported_message = (
            f"Supported versions for kr8s {__version__} are "
            f"{KUBERNETES_MINIMUM_SUPPORTED_VERSION}"
            " to "
            f"{KUBERNETES_MAXIMUM_SUPPORTED_VERSION}."
        )

        try:
            # Remove variant suffix if present before parsing, e.g v1.32.9-eks-113cf36 -> v1.32.9
            parsed = parse_version(git_version.split("-")[0])
        except InvalidVersion:
            warnings.warn(
                f"Unable to parse Kubernetes version {git_version}. {supported_message}",
                UserWarning,
                stacklevel=3,
            )
            return

        # We only care about major/minor version differences, so we truncate the patch version
        parsed = parse_version(f"{parsed.major}.{parsed.minor}")
        if (
            parsed < KUBERNETES_MINIMUM_SUPPORTED_VERSION
            or parsed > KUBERNETES_MAXIMUM_SUPPORTED_VERSION
        ):
            warnings.warn(
                f"Kubernetes version {git_version} is not supported. {supported_message}",
                UserWarning,
                stacklevel=3,
            )

    def _tokenreview_payload(self) -> str:
        return json.dumps(
            {
                "apiVersion": "authentication.k8s.io/v1",
                "kind": "TokenReview",
                "spec": {"token": self.auth.token},
            }
        )

    def _whoami_from_client_cert(self) -> str | None:
        if not self.auth.client_cert_file:
            return None
        with open(self.auth.client_cert_file, "rb") as f:
            cert = x509.load_pem_x509_certificate(f.read())
            [name] = cert.subject.get_attributes_for_oid(x509.OID_COMMON_NAME)
            return str(name.value)

    @staticmethod
    def _match_kind(resources: list[dict], kind: str) -> tuple[str, str, bool] | None:
        from ._objects import parse_kind

        kind, group, version = parse_kind(kind)
        if group:
            version = f"{group}/{version}"
        for resource in resources:
            if (not version or version in resource["version"]) and (
                kind == resource["name"]
                or kind == resource["kind"]
                or kind == resource["singularName"]
                or ("shortNames" in resource and kind in resource["shortNames"])
            ):
                if "/" in resource["version"]:
                    return (
                        f"{resource['singularName']}.{resource['version']}",
                        resource["name"],
                        resource["namespaced"],
                    )
                return (
                    f"{resource['singularName']}/{resource['version']}",
                    resource["name"],
                    resource["namespaced"],
                )
        return None

    def _api_resources_cache_valid(self) -> bool:
        return (
            self._api_resources is not None
            and time.monotonic() < self._api_resources_expiry
        )

    def _set_api_resources(self, resources: list[dict]) -> list[dict]:
        self._api_resources = resources
        self._api_resources_expiry = time.monotonic() + API_RESOURCES_CACHE_TTL
        return resources

    @staticmethod
    def _version_resources(version: str, resource_list: dict) -> list[dict]:
        return [
            {"version": version, **r}
            for r in resource_list["resources"]
            if "/" not in r["name"]
        ]

    @staticmethod
    def _group_versions(api_list: dict) -> list[str]:
        """Return the group versions to query for resources, in lookup priority order."""
        return [
            api_version["groupVersion"]
            for api in sorted(api_list["groups"], key=lambda d: d["name"])
            for api_version in sort_versions(
                api["versions"], key=lambda x: x["groupVersion"]
            )
        ]

    def _get_kind_params(
        self,
        namespace: str | None,
        label_selector: str | dict | None,
        field_selector: str | dict | None,
        params: dict | None,
        watch: bool,
        kwargs: dict,
    ) -> tuple[str, dict]:
        if not namespace:
            namespace = self.namespace
        if namespace is ALL:
            namespace = ""
        if params is None:
            params = {}
        if label_selector:
            if isinstance(label_selector, dict):
                label_selector = dict_to_selector(label_selector)
            params["labelSelector"] = label_selector
        if field_selector:
            if isinstance(field_selector, dict):
                field_selector = dict_to_selector(field_selector)
            params["fieldSelector"] = field_selector
        if watch:
            params["watch"] = "true" if watch else "false"
            kwargs["stream"] = True
        return namespace, params

    def _object_class(
        self,
        kind: str,
        lookup: tuple[str, str, bool] | None,
        allow_unknown_type: bool,
    ) -> type[BaseAPIObject]:
        """Find or create the object class for a kind returned by :meth:`lookup_kind`."""
        from ._objects import get_class, new_class

        plural = None
        namespaced = None
        if lookup:
            kind, plural, namespaced = lookup
        try:
            return get_class(kind, _asyncio=self._asyncio)
        except KeyError:
            if not allow_unknown_type:
                raise
            if namespaced is not None:
                return new_class(
                    kind, namespaced=namespaced, asyncio=self._asyncio, plural=plural
                )
            return new_class(kind, asyncio=self._asyncio, plural=plural)

    @staticmethod
    def _get_single_request(
        name: str | None,
        field_selector: str | dict | None,
        as_object: type[BaseAPIObject] | None,
    ) -> tuple[str | dict | None, dict, dict]:
        """Build the field selector, headers and params for getting one name."""
        if name is not None:
            # Normalized field_selector to a string
            field_selector_str: str
            if isinstance(field_selector, dict):
                field_selector_str = dict_to_selector(field_selector)
            elif field_selector is None:
                field_selector_str = ""
            else:
                field_selector_str = field_selector
            field_selector = f"metadata.name={name},{field_selector_str}"

        headers = {}
        params: dict[str, Any] = {}
        if as_object:
            group, version = as_object.version.split("/")
            headers["Accept"] = (
                f"application/json;as={as_object.kind};v={version};g={group}"
            )
        else:
            params["limit"] = 100
        return field_selector, headers, params

    def _page_items(
        self,
        resourcelist: dict,
        obj_cls: type[BaseAPIObject],
        name: str | None,
        as_object: type[BaseAPIObject] | None,
        raw: bool,
    ) -> list[BaseAPIObject | dict]:
        """Turn one page of a list response into objects or dicts."""
        if (
            as_object
            and "kind" in resourcelist
            and resourcelist["kind"] == as_object.kind
        ):
            return [resourcelist if raw else as_object(resourcelist, api=self)]
        items: list[BaseAPIObject | dict] = []
        for item in resourcelist.get("items", []):
            if name is None or item["metadata"]["name"] == name:
                items.append(item if raw else obj_cls(item, api=self))
        return items

    @staticmethod
    def _next_page(resourcelist: dict, params: dict) -> bool:
        """Set the continue token for the next page, and return whether there is one."""
        if (
            "metadata" in resourcelist
            and "continue" in resourcelist["metadata"]
            and resourcelist["metadata"]["continue"]
        ):
            params["continue"] = resourcelist["metadata"]["continue"]
            return True
        return False

    def _parse_watch_event(
        self, line: str, obj_cls: type[BaseAPIObject], kind: str, since: str | None
    ) -> tuple[str, BaseAPIObject] | None:
        """Parse a watch event, returning None if the watch must restart."""
        event = json.loads(line)
        if event["object"]["kind"] == "Status" and event["object"].get("code") == 410:
            logger.debug(
                f"Got 410 Gone: Restarting watch of {kind} at resourceVersion {since}"
            )
            return None
        return event["type"], obj_cls(event["object"], api=self)

    @property
    def __version__(self) -> str:
        return f"kr8s/{__version__}"

    @property
    def namespace(self) -> str:
        """Get the default namespace."""
        assert self.auth.namespace
        return self.auth.namespace

    @namespace.setter
    def namespace(self, value):
        self.auth.namespace = value


class Api(BaseApi[KubeAuth, httpx2.Client]):
    """A kr8s object for interacting with the Kubernetes API using blocking I/O.

    .. warning::
        This class is not intended to be instantiated directly. Instead, use the
        :func:`kr8s.api` function to get a singleton instance of the API.

        See https://docs.kr8s.org/en/stable/client.html#client-caching.

    """

    _asyncio = False
    _auth_cls = KubeAuth
    _instances: dict[str, weakref.WeakValueDictionary] = {}

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._api_resources_lock = threading.Lock()

    def _initialize(self) -> Api:
        self.auth.reauthenticate()
        self._check_version()
        return self

    def _create_session(self) -> None:
        if not self.auth.server:
            self.auth.reauthenticate()
        if self._session:
            with contextlib.suppress(RuntimeError):
                self._session.close()
            self._session = None
        self._session = httpx2.Client(**self._session_kwargs(self.auth.ssl_context()))

    @contextlib.contextmanager
    def call_api(
        self,
        method: str = "GET",
        version: str = "v1",
        base: str = "",
        namespace: str | None = None,
        url: str = "",
        raise_for_status: bool = True,
        stream: bool = False,
        **kwargs,
    ) -> Generator[httpx2.Response]:
        """Make a Kubernetes API request."""
        if not self._session or self._session.is_closed:
            self._create_session()
        kwargs = self._request_kwargs(version, base, namespace, url, kwargs)
        kwargs.update(method=method)
        auth_attempts = 0
        ssl_attempts = 0
        while True:
            try:
                assert self._session
                if stream:
                    with self._session.stream(**kwargs) as response:
                        if response.is_error and raise_for_status:
                            # NOTE: Avoid `httpx2.ResponseNotRead` w/ streaming requests
                            # https://github.com/encode/httpx/discussions/1856#discussioncomment-1316674
                            response.read()
                            response.raise_for_status()
                        yield response
                else:
                    response = self._session.request(**kwargs)
                    if raise_for_status:
                        response.raise_for_status()
                    yield response
            except httpx2.HTTPStatusError as e:
                if self._should_reauthenticate(e, auth_attempts):
                    auth_attempts += 1
                    self.auth.reauthenticate()
                    self._create_session()
                    continue
                if error := self._server_error(e):
                    raise error from e
                raise
            except ssl.SSLCertVerificationError:
                # In some rare edge cases the SSL verification fails, so we try again
                # a few times before giving up.
                if ssl_attempts < MAX_RETRIES:
                    ssl_attempts += 1
                    self.auth.reauthenticate()
                    self._create_session()
                    continue
                raise
            except httpx2.TimeoutException as e:
                raise APITimeoutError(
                    "Timeout while waiting for the Kubernetes API server"
                ) from e
            break

    @contextlib.contextmanager
    def open_websocket(
        self,
        version: str = "v1",
        base: str = "",
        namespace: str | None = None,
        url: str = "",
        **kwargs,
    ) -> Generator[websockets.WebSocketSession]:
        """Open a websocket connection to a Kubernetes API endpoint."""
        if not self._session or self._session.is_closed:
            self._create_session()
        kwargs = self._request_kwargs(version, base, namespace, url, kwargs)
        auth_attempts = 0
        while True:
            try:
                with _httpx_ws_exception_fixer():
                    assert self._session
                    with websockets.WebSocketClient(self._session).connect(
                        **kwargs
                    ) as response:
                        yield response
            except websockets.WebSocketDisconnect as e:
                retry = self._should_retry_websocket(e, auth_attempts)
                if retry:
                    auth_attempts += 1
                    self.auth.reauthenticate()
                    continue
                if retry is False:
                    raise
            break

    def version(self) -> dict:
        """Get the Kubernetes version information from the API.

        Returns:
            The Kubernetes version information.

        """
        with self.call_api(method="GET", version="", base="/version") as response:
            return response.json()

    def _check_version(self) -> None:
        self._warn_if_unsupported(self.version())

    def reauthenticate(self) -> None:
        """Reauthenticate the API."""
        self.auth.reauthenticate()

    def whoami(self):
        """Retrieve the subject that's currently authenticated.

        Inspired by `kubectl whoami`.

        Returns:
            str: The subject that's currently authenticated.
        """
        if self.auth.token:
            with self.call_api(
                "POST",
                version="authentication.k8s.io/v1",
                url="tokenreviews",
                content=self._tokenreview_payload(),
            ) as r:
                return r.json()["status"]["user"]["username"]
        return self._whoami_from_client_cert()

    def lookup_kind(self, kind, skip_cache: bool = False) -> tuple[str, str, bool]:
        """Lookup a Kubernetes resource kind.

        Check whether a resource kind exists on the remote server.

        Args:
            kind: The kind of resource to lookup.
            skip_cache: Fetch the API resources from the server instead of using the cache.

        Returns:
            The kind of resource, the plural form and whether the resource is namespaced

        Raises:
            ValueError: If the kind is not found.
        """
        if skip_cache:
            resources = self.api_resources_uncached()
        else:
            resources = self.api_resources()
        if match := self._match_kind(resources, kind):
            return match
        if not skip_cache:
            return self.lookup_kind(kind, skip_cache=True)
        raise ValueError(f"Kind {kind} not found.")

    @contextlib.contextmanager
    def get_kind(
        self,
        kind: str | type[BaseAPIObject],
        namespace: str | None = None,
        label_selector: str | dict | None = None,
        field_selector: str | dict | None = None,
        params: dict | None = None,
        watch: bool = False,
        allow_unknown_type: bool = True,
        **kwargs,
    ) -> Generator[tuple[type[BaseAPIObject], httpx2.Response]]:
        """Get a Kubernetes resource."""
        namespace, params = self._get_kind_params(
            namespace, label_selector, field_selector, params, watch, kwargs
        )
        if isinstance(kind, type):
            obj_cls = kind
        else:
            lookup = None
            try:
                lookup = self.lookup_kind(kind)
            except ServerError as e:
                warnings.warn(str(e), stacklevel=1)
            obj_cls = self._object_class(kind, lookup, allow_unknown_type)
        with self.call_api(
            method="GET",
            url=obj_cls.endpoint,
            version=obj_cls.version,
            namespace=namespace if obj_cls.namespaced else None,
            params=params or None,
            **kwargs,
        ) as response:
            yield obj_cls, response

    def get(
        self,
        kind: str | type,
        *names: str,
        namespace: str | None = None,
        label_selector: str | dict | None = None,
        field_selector: str | dict | None = None,
        as_object: type[BaseAPIObject] | None = None,
        allow_unknown_type: bool = True,
        raw: bool = False,
        **kwargs,
    ) -> Generator[BaseAPIObject | dict]:
        """Get Kubernetes resources.

        Args:
            kind: The kind of resource to get.
            *names: The names of specific resources to get.
            namespace: The namespace to get the resource from.
            label_selector: The label selector to filter the resources by.
            field_selector: The field selector to filter the resources by.
            as_object: The object to return the resources as.
            allow_unknown_type: Automatically create a class for the resource if none exists, default True.
            raw: If True, return raw dictionaries instead of APIObject instances, default False.
            **kwargs: Additional keyword arguments to pass to the API call.

        Returns:
            The resources.
        """
        for name in names or [None]:
            yield from self._get_single(
                kind,
                name,
                namespace=namespace,
                label_selector=label_selector,
                field_selector=field_selector,
                as_object=as_object,
                allow_unknown_type=allow_unknown_type,
                raw=raw,
                **kwargs,
            )

    def _get_single(
        self,
        kind: str | type,
        name: str | None = None,
        namespace: str | None = None,
        label_selector: str | dict | None = None,
        field_selector: str | dict | None = None,
        as_object: type[BaseAPIObject] | None = None,
        allow_unknown_type: bool = True,
        raw: bool = False,
        **kwargs,
    ) -> Generator[BaseAPIObject | dict]:
        field_selector, headers, params = self._get_single_request(
            name, field_selector, as_object
        )
        continue_paging = True
        while continue_paging:
            with self.get_kind(
                kind,
                namespace=namespace,
                label_selector=label_selector,
                field_selector=field_selector,
                headers=headers or None,
                allow_unknown_type=allow_unknown_type,
                params=params,
                **kwargs,
            ) as (obj_cls, response):
                resourcelist = response.json()
                yield from self._page_items(resourcelist, obj_cls, name, as_object, raw)
                continue_paging = self._next_page(resourcelist, params)

    def watch(
        self,
        kind: str,
        namespace: str | None = None,
        label_selector: str | dict | None = None,
        field_selector: str | dict | None = None,
        since: str | None = None,
        allow_unknown_type: bool = True,
        timeout: float | None = None,
    ) -> Generator[tuple[str, BaseAPIObject]]:
        """Watch a Kubernetes resource."""
        while True:
            restart_watch = False
            with self.get_kind(
                kind,
                namespace=namespace,
                label_selector=label_selector,
                field_selector=field_selector,
                params={"resourceVersion": since} if since else None,
                watch=True,
                timeout=timeout,
                allow_unknown_type=allow_unknown_type,
            ) as (obj_cls, response):
                logger.debug(
                    f"Starting watch of {kind}{' at resourceVersion ' + since if since else ''}"
                )
                for line in response.iter_lines():
                    event = self._parse_watch_event(line, obj_cls, kind, since)
                    if event is None:
                        restart_watch = True
                        break
                    since = event[1].metadata.resourceVersion
                    yield event
            if not restart_watch:
                return

    def api_resources(self) -> list[dict]:
        """Get the Kubernetes API resources."""
        with self._api_resources_lock:
            if not self._api_resources_cache_valid():
                self._set_api_resources(self.api_resources_uncached())
            assert self._api_resources is not None
            return self._api_resources

    def api_resources_uncached(self) -> list[dict]:
        """Get the Kubernetes API resources."""
        resources = []
        with self.call_api(method="GET", version="", base="/api") as response:
            core_api_list = response.json()
        for version in core_api_list["versions"]:
            with self.call_api(
                method="GET", version="", base="/api", url=version
            ) as response:
                resources.extend(self._version_resources(version, response.json()))
        with self.call_api(method="GET", version="", base="/apis") as response:
            api_list = response.json()
        for version in self._group_versions(api_list):
            with self.call_api(
                method="GET", version="", base="/apis", url=version
            ) as response:
                resources.extend(self._version_resources(version, response.json()))
        return resources

    def api_versions(self) -> Generator[str]:
        """Get the Kubernetes API versions."""
        with self.call_api(method="GET", version="", base="/api") as response:
            core_api_list = response.json()
        yield from core_api_list["versions"]

        with self.call_api(method="GET", version="", base="/apis") as response:
            api_list = response.json()
        for group in api_list["groups"]:
            for version in group["versions"]:
                yield version["groupVersion"]

    def create(self, resources: list) -> None:
        """Create resources in the Kubernetes cluster."""
        for resource in resources:
            resource.create()


class AsyncApi(BaseApi[AsyncKubeAuth, httpx2.AsyncClient]):
    """A kr8s object for interacting with the Kubernetes API using asyncio or trio.

    .. warning::
        This class is not intended to be instantiated directly. Instead, use the
        :func:`kr8s.asyncio.api` function to get a singleton instance of the API.

        See https://docs.kr8s.org/en/stable/client.html#client-caching.

    """

    _asyncio = True
    _auth_cls = AsyncKubeAuth
    _instances: dict[str, weakref.WeakValueDictionary] = {}

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._api_resources_lock = anyio.Lock()

    def __await__(self):
        async def f():
            await self.auth
            await self._check_version()
            return self

        return f().__await__()

    async def _create_session(self) -> None:
        if not self.auth.server:
            await self.auth.reauthenticate()
        if self._session:
            with contextlib.suppress(RuntimeError):
                await self._session.aclose()
            self._session = None
        self._session = httpx2.AsyncClient(
            **self._session_kwargs(await self.auth.ssl_context())
        )

    @contextlib.asynccontextmanager
    async def call_api(
        self,
        method: str = "GET",
        version: str = "v1",
        base: str = "",
        namespace: str | None = None,
        url: str = "",
        raise_for_status: bool = True,
        stream: bool = False,
        **kwargs,
    ) -> AsyncGenerator[httpx2.Response]:
        """Make a Kubernetes API request."""
        if not self._session or self._session.is_closed:
            await self._create_session()
        kwargs = self._request_kwargs(version, base, namespace, url, kwargs)
        kwargs.update(method=method)
        auth_attempts = 0
        ssl_attempts = 0
        while True:
            try:
                assert self._session
                if stream:
                    async with self._session.stream(**kwargs) as response:
                        if response.is_error and raise_for_status:
                            # NOTE: Avoid `httpx2.ResponseNotRead` w/ streaming requests
                            # https://github.com/encode/httpx/discussions/1856#discussioncomment-1316674
                            await response.aread()
                            response.raise_for_status()
                        yield response
                else:
                    response = await self._session.request(**kwargs)
                    if raise_for_status:
                        response.raise_for_status()
                    yield response
            except httpx2.HTTPStatusError as e:
                if self._should_reauthenticate(e, auth_attempts):
                    auth_attempts += 1
                    await self.auth.reauthenticate()
                    await self._create_session()
                    continue
                if error := self._server_error(e):
                    raise error from e
                raise
            except ssl.SSLCertVerificationError:
                # In some rare edge cases the SSL verification fails, so we try again
                # a few times before giving up.
                if ssl_attempts < MAX_RETRIES:
                    ssl_attempts += 1
                    await self.auth.reauthenticate()
                    await self._create_session()
                    continue
                raise
            except httpx2.TimeoutException as e:
                raise APITimeoutError(
                    "Timeout while waiting for the Kubernetes API server"
                ) from e
            break

    @contextlib.asynccontextmanager
    async def open_websocket(
        self,
        version: str = "v1",
        base: str = "",
        namespace: str | None = None,
        url: str = "",
        **kwargs,
    ) -> AsyncGenerator[websockets.AsyncWebSocketSession]:
        """Open a websocket connection to a Kubernetes API endpoint."""
        if not self._session or self._session.is_closed:
            await self._create_session()
        kwargs = self._request_kwargs(version, base, namespace, url, kwargs)
        auth_attempts = 0
        while True:
            try:
                with _httpx_ws_exception_fixer():
                    assert self._session
                    async with websockets.AsyncWebSocketClient(self._session).connect(
                        **kwargs
                    ) as response:
                        yield response
            except websockets.WebSocketDisconnect as e:
                retry = self._should_retry_websocket(e, auth_attempts)
                if retry:
                    auth_attempts += 1
                    await self.auth.reauthenticate()
                    continue
                if retry is False:
                    raise
            break

    async def version(self) -> dict:
        """Get the Kubernetes version information from the API.

        Returns:
            The Kubernetes version information.

        """
        async with self.call_api(method="GET", version="", base="/version") as response:
            return response.json()

    async def _check_version(self) -> None:
        self._warn_if_unsupported(await self.version())

    async def reauthenticate(self) -> None:
        """Reauthenticate the API."""
        await self.auth.reauthenticate()

    async def whoami(self):
        """Retrieve the subject that's currently authenticated.

        Inspired by `kubectl whoami`.

        Returns:
            str: The subject that's currently authenticated.
        """
        if self.auth.token:
            async with self.call_api(
                "POST",
                version="authentication.k8s.io/v1",
                url="tokenreviews",
                content=self._tokenreview_payload(),
            ) as r:
                return r.json()["status"]["user"]["username"]
        return self._whoami_from_client_cert()

    async def lookup_kind(
        self, kind, skip_cache: bool = False
    ) -> tuple[str, str, bool]:
        """Lookup a Kubernetes resource kind.

        Check whether a resource kind exists on the remote server.

        Args:
            kind: The kind of resource to lookup.
            skip_cache: Fetch the API resources from the server instead of using the cache.

        Returns:
            The kind of resource, the plural form and whether the resource is namespaced

        Raises:
            ValueError: If the kind is not found.
        """
        if skip_cache:
            resources = await self.api_resources_uncached()
        else:
            resources = await self.api_resources()
        if match := self._match_kind(resources, kind):
            return match
        if not skip_cache:
            return await self.lookup_kind(kind, skip_cache=True)
        raise ValueError(f"Kind {kind} not found.")

    @contextlib.asynccontextmanager
    async def get_kind(
        self,
        kind: str | type[BaseAPIObject],
        namespace: str | None = None,
        label_selector: str | dict | None = None,
        field_selector: str | dict | None = None,
        params: dict | None = None,
        watch: bool = False,
        allow_unknown_type: bool = True,
        **kwargs,
    ) -> AsyncGenerator[tuple[type[BaseAPIObject], httpx2.Response]]:
        """Get a Kubernetes resource."""
        namespace, params = self._get_kind_params(
            namespace, label_selector, field_selector, params, watch, kwargs
        )
        if isinstance(kind, type):
            obj_cls = kind
        else:
            lookup = None
            try:
                lookup = await self.lookup_kind(kind)
            except ServerError as e:
                warnings.warn(str(e), stacklevel=1)
            obj_cls = self._object_class(kind, lookup, allow_unknown_type)
        async with self.call_api(
            method="GET",
            url=obj_cls.endpoint,
            version=obj_cls.version,
            namespace=namespace if obj_cls.namespaced else None,
            params=params or None,
            **kwargs,
        ) as response:
            yield obj_cls, response

    async def get(
        self,
        kind: str | type,
        *names: str,
        namespace: str | None = None,
        label_selector: str | dict | None = None,
        field_selector: str | dict | None = None,
        as_object: type[BaseAPIObject] | None = None,
        allow_unknown_type: bool = True,
        raw: bool = False,
        **kwargs,
    ) -> AsyncGenerator[BaseAPIObject | dict]:
        """Get Kubernetes resources.

        Args:
            kind: The kind of resource to get.
            *names: The names of specific resources to get.
            namespace: The namespace to get the resource from.
            label_selector: The label selector to filter the resources by.
            field_selector: The field selector to filter the resources by.
            as_object: The object to return the resources as.
            allow_unknown_type: Automatically create a class for the resource if none exists, default True.
            raw: If True, return raw dictionaries instead of APIObject instances, default False.
            **kwargs: Additional keyword arguments to pass to the API call.

        Returns:
            The resources.
        """
        for name in names or [None]:
            async for resource in self._get_single(
                kind,
                name,
                namespace=namespace,
                label_selector=label_selector,
                field_selector=field_selector,
                as_object=as_object,
                allow_unknown_type=allow_unknown_type,
                raw=raw,
                **kwargs,
            ):
                yield resource

    async def _get_single(
        self,
        kind: str | type,
        name: str | None = None,
        namespace: str | None = None,
        label_selector: str | dict | None = None,
        field_selector: str | dict | None = None,
        as_object: type[BaseAPIObject] | None = None,
        allow_unknown_type: bool = True,
        raw: bool = False,
        **kwargs,
    ) -> AsyncGenerator[BaseAPIObject | dict]:
        field_selector, headers, params = self._get_single_request(
            name, field_selector, as_object
        )
        continue_paging = True
        while continue_paging:
            async with self.get_kind(
                kind,
                namespace=namespace,
                label_selector=label_selector,
                field_selector=field_selector,
                headers=headers or None,
                allow_unknown_type=allow_unknown_type,
                params=params,
                **kwargs,
            ) as (obj_cls, response):
                resourcelist = response.json()
                for item in self._page_items(
                    resourcelist, obj_cls, name, as_object, raw
                ):
                    yield item
                continue_paging = self._next_page(resourcelist, params)

    async def watch(
        self,
        kind: str,
        namespace: str | None = None,
        label_selector: str | dict | None = None,
        field_selector: str | dict | None = None,
        since: str | None = None,
        allow_unknown_type: bool = True,
    ) -> AsyncGenerator[tuple[str, BaseAPIObject]]:
        """Watch a Kubernetes resource."""
        while True:
            restart_watch = False
            async with self.get_kind(
                kind,
                namespace=namespace,
                label_selector=label_selector,
                field_selector=field_selector,
                params={"resourceVersion": since} if since else None,
                watch=True,
                timeout=None,
                allow_unknown_type=allow_unknown_type,
            ) as (obj_cls, response):
                logger.debug(
                    f"Starting watch of {kind}{' at resourceVersion ' + since if since else ''}"
                )
                async for line in response.aiter_lines():
                    event = self._parse_watch_event(line, obj_cls, kind, since)
                    if event is None:
                        restart_watch = True
                        break
                    since = event[1].metadata.resourceVersion
                    yield event
            if not restart_watch:
                return

    async def api_resources(self) -> list[dict]:
        """Get the Kubernetes API resources."""
        async with self._api_resources_lock:
            if not self._api_resources_cache_valid():
                self._set_api_resources(await self.api_resources_uncached())
            assert self._api_resources is not None
            return self._api_resources

    async def api_resources_uncached(self) -> list[dict]:
        """Get the Kubernetes API resources."""
        resources = []
        async with self.call_api(method="GET", version="", base="/api") as response:
            core_api_list = response.json()
        for version in core_api_list["versions"]:
            async with self.call_api(
                method="GET", version="", base="/api", url=version
            ) as response:
                resources.extend(self._version_resources(version, response.json()))
        async with self.call_api(method="GET", version="", base="/apis") as response:
            api_list = response.json()
        for version in self._group_versions(api_list):
            async with self.call_api(
                method="GET", version="", base="/apis", url=version
            ) as response:
                resources.extend(self._version_resources(version, response.json()))
        return resources

    async def api_versions(self) -> AsyncGenerator[str]:
        """Get the Kubernetes API versions."""
        async with self.call_api(method="GET", version="", base="/api") as response:
            core_api_list = response.json()
        for version in core_api_list["versions"]:
            yield version

        async with self.call_api(method="GET", version="", base="/apis") as response:
            api_list = response.json()
        for group in api_list["groups"]:
            for version in group["versions"]:
                yield version["groupVersion"]

    async def create(self, resources: list):
        """Create resources in the Kubernetes cluster."""
        async with anyio.create_task_group() as tg:
            for resource in resources:
                tg.start_soon(resource.create)


def hash_kwargs(kwargs: dict):
    key_kwargs = copy.copy(kwargs)
    for key in key_kwargs:
        if isinstance(key_kwargs[key], dict):
            key_kwargs[key] = json.dumps(key_kwargs[key])
    return frozenset(key_kwargs.items())
