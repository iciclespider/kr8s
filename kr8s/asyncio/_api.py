# SPDX-FileCopyrightText: Copyright (c) 2023-2026, Kr8s Developers (See LICENSE for list)
# SPDX-License-Identifier: BSD 3-Clause License
from typing import cast

from kr8s._api import AsyncApi
from kr8s._types import PathType


async def api(
    url: str | None = None,
    kubeconfig: PathType | None = None,
    serviceaccount: str | None = None,
    namespace: str | None = None,
    context: str | None = None,
) -> AsyncApi:
    """Create a `kr8s.asyncio.Api` object for interacting with the Kubernetes API.

    If a kr8s object already exists with the same arguments in this thread, it will be returned.

    Args:
        url: The URL of the Kubernetes API server
        kubeconfig: The path to a kubeconfig file to use
        serviceaccount: The path of a service account to use
        namespace: The namespace to use
        context: The context to use

    Returns:
        kr8s.asyncio.Api: The API object

    Examples:
        >>> import kr8s
        >>> api = await kr8s.asyncio.api()  # Uses the default kubeconfig
        >>> print(await api.version())  # Get the Kubernetes version
    """
    kwargs = dict(
        url=url,
        kubeconfig=kubeconfig,
        serviceaccount=serviceaccount,
        namespace=namespace,
        context=context,
    )
    instance = cast(AsyncApi | None, AsyncApi._cached_instance(**kwargs))
    if instance is None:
        instance = AsyncApi(**kwargs, bypass_factory=True)
    return await instance
