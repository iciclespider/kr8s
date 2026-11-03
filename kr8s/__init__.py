# SPDX-FileCopyrightText: Copyright (c) 2023-2026, Kr8s Developers (See LICENSE for list)
# SPDX-License-Identifier: BSD 3-Clause License
"""
This module contains `kr8s`, a simple, extensible Python client library for Kubernetes.

At the top level, `kr8s` provides a synchronous API. An asynchronous API is provided by `kr8s.asyncio`.
Both APIs are functionally identical with the same objects, method signatures and return values.
"""

from __future__ import annotations

from collections.abc import Generator
from typing import cast

from . import asyncio, objects, portforward
from ._api import ALL, Api
from ._constants import (
    KUBERNETES_MAXIMUM_SUPPORTED_VERSION,
    KUBERNETES_MINIMUM_SUPPORTED_VERSION,
)
from ._exceptions import (
    APITimeoutError,
    ConnectionClosedError,
    ExecError,
    NotFoundError,
    ServerError,
)
from ._types import PathType

try:
    from ._version import version as __version__  # noqa
    from ._version import version_tuple as __version_tuple__  # noqa
except ImportError:
    __version__ = "0.0.0"
    __version_tuple__ = (0, 0, 0)


def api(
    url: str | None = None,
    kubeconfig: PathType | None = None,
    serviceaccount: str | None = None,
    namespace: str | None = None,
    context: str | None = None,
) -> Api:
    """Create a :class:`kr8s.Api` object for interacting with the Kubernetes API.

    If a kr8s object already exists with the same arguments in this thread, it will be returned.

    Args:
        url: The URL of the Kubernetes API server
        kubeconfig: The path to a kubeconfig file to use
        serviceaccount: The path of a service account to use
        namespace: The namespace to use
        context: The context to use

    Returns:
        The API object

    Examples:
        >>> import kr8s
        >>> api = kr8s.api()  # Uses the default kubeconfig
        >>> print(api.version())  # Get the Kubernetes version
    """
    kwargs = dict(
        url=url,
        kubeconfig=kubeconfig,
        serviceaccount=serviceaccount,
        namespace=namespace,
        context=context,
    )
    instance = cast(Api | None, Api._cached_instance(**kwargs))
    if instance is None:
        instance = Api(**kwargs, bypass_factory=True)
    return instance._initialize()


def _default_api(client: Api | None) -> Api:
    """Return the given client, or the default one when it is None."""
    return client if client is not None else api()


def get(
    kind: str,
    *names: str,
    namespace: str | None = None,
    label_selector: str | dict | None = None,
    field_selector: str | dict | None = None,
    as_object: type | None = None,
    allow_unknown_type: bool = True,
    raw: bool = False,
    api=None,
    **kwargs,
):
    """Get a resource by name.

    Args:
        kind: The kind of resource to get
        *names: The names of the resources to get
        namespace: The namespace to get the resource from
        label_selector: The label selector to filter the resources by
        field_selector: The field selector to filter the resources by
        as_object: The object to populate with the resource data
        allow_unknown_type: Whether to allow unknown types
        raw: If True, return raw dictionaries instead of APIObject instances, default False
        api: The api to use to get the resource
        **kwargs: Additional arguments to pass to the API

    Returns:
        The populated object (or dict if raw=True)

    Raises:
        ValueError: If the resource is not found

    Examples:
        >>> import kr8s
        >>> # All of these are equivalent
        >>> ings = kr8s.get("ing")                           # Short name
        >>> ings = kr8s.get("ingress")                       # Singular
        >>> ings = kr8s.get("ingresses")                     # Plural
        >>> ings = kr8s.get("Ingress")                       # Title
        >>> ings = kr8s.get("ingress.networking.k8s.io")     # Full group name
        >>> ings = kr8s.get("ingress.v1.networking.k8s.io")  # Full with explicit version
        >>> ings = kr8s.get("ingress.networking.k8s.io/v1")  # Full with explicit version alt.
        >>>
        >>> # Get raw dictionaries for better performance
        >>> pods = list(kr8s.get("pods", raw=True))
        >>> print(pods[0]["metadata"]["name"])
    """
    api = _default_api(api)
    return api.get(
        kind,
        *names,
        namespace=namespace,
        label_selector=label_selector,
        field_selector=field_selector,
        as_object=as_object,
        allow_unknown_type=allow_unknown_type,
        raw=raw,
        **kwargs,
    )


def version(api: Api | None = None) -> dict:
    """Get the Kubernetes version information from the API.

    Returns:
        The Kubernetes version information.
    """
    api = _default_api(api)
    return api.version()


def watch(
    kind: str,
    namespace: str | None = None,
    label_selector: str | dict | None = None,
    field_selector: str | dict | None = None,
    since: str | None = None,
    api: Api | None = None,
) -> Generator[tuple[str, objects.APIObject]]:
    """Watch a Kubernetes resource."""
    api = _default_api(api)
    for event, obj in api.watch(
        kind,
        namespace=namespace,
        label_selector=label_selector,
        field_selector=field_selector,
        since=since,
    ):
        yield event, cast(objects.APIObject, obj)


def api_resources(api: Api | None = None) -> list[dict]:
    """Get the Kubernetes API resources."""
    api = _default_api(api)
    return api.api_resources()


def whoami(api: Api | None = None):
    """Get the current user's identity.

    Returns:
        The user's identity

    Examples:
        >>> import kr8s
        >>> print(kr8s.whoami())
    """
    api = _default_api(api)
    return api.whoami()


def create(resources: list[objects.APIObject], api: Api | None = None) -> None:
    """Creates resources in the Kubernetes cluster."""
    api = _default_api(api)
    api.create(resources)


__all__ = [
    "__version__",
    "__version_tuple__",
    "ALL",
    "api",
    "api_resources",
    "asyncio",
    "create",
    "get",
    "objects",
    "portforward",
    "version",
    "watch",
    "whoami",
    "Api",
    "APITimeoutError",
    "ConnectionClosedError",
    "ExecError",
    "NotFoundError",
    "ServerError",
    "KUBERNETES_MINIMUM_SUPPORTED_VERSION",
    "KUBERNETES_MAXIMUM_SUPPORTED_VERSION",
]
