# SPDX-FileCopyrightText: Copyright (c) 2024-2026, Kr8s Developers (See LICENSE for list)
# SPDX-License-Identifier: BSD 3-Clause License
"""Objects to represent Kubernetes resources.

This module provides classes that represent Kubernetes resources.
These classes are used to interact with resources in the Kubernetes API server.
"""

# Disable missing docstrings, these match the async version of the objects
# ruff: noqa: D102, D107
from __future__ import annotations

import contextlib
import json
import time
from collections.abc import Generator
from typing import (
    Any,
    BinaryIO,
    Literal,
    Self,
    cast,
)

import httpx2
import yaml

from ._api import Api
from ._exceptions import APITimeoutError, NotFoundError, ServerError
from ._exec import CompletedExec, Exec
from ._objects import (
    BaseAPIObject,
    BaseBinding,
    BaseClusterRole,
    BaseClusterRoleBinding,
    BaseComponentStatus,
    BaseConfigMap,
    BaseControllerRevision,
    BaseCronJob,
    BaseCustomResourceDefinition,
    BaseDaemonSet,
    BaseDeployment,
    BaseEndpoints,
    BaseEvent,
    BaseHorizontalPodAutoscaler,
    BaseIngress,
    BaseIngressClass,
    BaseIPAddress,
    BaseJob,
    BaseLimitRange,
    BaseNamespace,
    BaseNetworkPolicy,
    BaseNode,
    BasePersistentVolume,
    BasePersistentVolumeClaim,
    BasePod,
    BasePodDisruptionBudget,
    BasePodTemplate,
    BaseReplicaSet,
    BaseReplicationController,
    BaseResourceQuota,
    BaseRole,
    BaseRoleBinding,
    BaseSecret,
    BaseService,
    BaseServiceAccount,
    BaseServiceCIDR,
    BaseStatefulSet,
    BaseTable,
    find_object_files,
    parse_name_type,
)
from ._objects import get_class as _get_class
from ._objects import new_class as _new_class
from ._objects import object_from_spec as _object_from_spec
from .portforward import LocalPortType, PortForward


def _api(**kwargs) -> Api:
    import kr8s

    return kr8s.api(**kwargs)


class APIObject(BaseAPIObject):
    """Base class for Kubernetes objects."""

    _asyncio = False
    _api: Api | None

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if self._api is None:
            self._api = _api()

    @classmethod
    def get(
        cls,
        name: str | None = None,
        namespace: str | None = None,
        api: Api | None = None,
        label_selector: str | dict[str, str] | None = None,
        field_selector: str | dict[str, str] | None = None,
        timeout: int = 2,
        **kwargs,
    ) -> Self:
        """Get a Kubernetes resource by name or via selectors."""
        if api is None:
            api = _api()
        namespace = namespace if namespace else api.namespace
        start = time.time()
        backoff = 0.1
        while start + timeout > time.time():
            if name:
                try:
                    resources = list(
                        api.get(
                            cls,
                            name,
                            namespace=namespace,
                            field_selector={"metadata.name": name},
                            **kwargs,
                        )
                    )
                except ServerError as e:
                    if e.response and e.response.status_code == 404:
                        continue
                    raise e
            elif label_selector or field_selector:
                resources = list(
                    api.get(
                        cls,
                        namespace=namespace,
                        label_selector=label_selector,
                        field_selector=field_selector,
                        **kwargs,
                    )
                )
            else:
                raise ValueError("Must specify name or selector")
            if len(resources) == 0:
                time.sleep(backoff)
                backoff = min(backoff * 2, 1)
                continue
            if len(resources) > 1:
                raise ValueError(
                    f"Expected exactly one {cls.kind} object. Use selectors to narrow down the search."
                )
            assert isinstance(resources[0], cls)
            return resources[0]
        raise NotFoundError(
            f"Could not find {cls.kind} {name} in namespace {namespace}."
        )

    def exists(self, ensure=False) -> bool:
        """Check if this object exists in Kubernetes."""
        try:
            assert self.api
            with self.api.call_api(
                "GET",
                version=self.version,
                url=self._url,
                namespace=self.namespace,
                raise_for_status=False,
            ) as resp:
                status = resp.status_code
        except ValueError:
            status = 400
        if status == 200:
            return True
        if ensure:
            raise NotFoundError(
                f"Object {self.name} does not exist in {self.namespace}"
            )
        return False

    def create(self) -> None:
        """Create this object in Kubernetes."""
        assert self.api
        with self.api.call_api(
            "POST",
            version=self.version,
            url=self.endpoint,
            namespace=self.namespace,
            content=json.dumps(self.raw_template),
        ) as resp:
            self.raw = resp.json()

    def delete(
        self,
        propagation_policy: str | None = None,
        grace_period: int | None = None,
        force: bool = False,
    ) -> None:
        """Delete this object from Kubernetes.

        Args:
            propagation_policy: The deletion propagation policy.
            grace_period: The grace period for deletion.
            force: Force deletion. (Setting to ``True`` is equivelaent to setting grace_period to 0)
        """
        content = self._delete_payload(propagation_policy, grace_period, force)
        try:
            assert self.api
            with self.api.call_api(
                "DELETE",
                version=self.version,
                url=self._url,
                namespace=self.namespace,
                content=content,
            ):
                pass
        except ServerError as e:
            if not_found := self._not_found(e):
                raise not_found from e
            raise

    def refresh(self) -> None:
        """Refresh this object from Kubernetes."""
        try:
            assert self.api
            with self.api.call_api(
                "GET",
                version=self.version,
                url=self._url,
                namespace=self.namespace,
            ) as resp:
                self.raw = resp.json()
        except ServerError as e:
            if not_found := self._not_found(e):
                raise not_found from e
            raise

    def patch(self, patch: dict | list, *, subresource=None, type=None) -> None:
        """Patch this object in Kubernetes."""
        url, headers = self._patch_request(subresource, type)
        try:
            assert self.api
            with self.api.call_api(
                "PATCH",
                version=self.version,
                url=url,
                namespace=self.namespace,
                content=json.dumps(patch),
                headers=headers,
            ) as resp:
                self.raw = resp.json()
        except ServerError as e:
            if not_found := self._not_found(e):
                raise not_found from e
            raise

    def scale(self, replicas: int | None = None) -> None:
        """Scale this object in Kubernetes."""
        patch = self._scale_patch(replicas)
        self.exists(ensure=True)
        self.patch(patch)
        while self.replicas != replicas:
            self.refresh()
            time.sleep(0.1)

    def exec(
        self,
        command: list[str],
        *,
        container: str | None = None,
        stdin: str | BinaryIO | None = None,
        stdout: BinaryIO | None = None,
        stderr: BinaryIO | None = None,
        check: bool = True,
        capture_output: bool = True,
        timeout: int | None = None,
    ) -> CompletedExec:
        """Execute a command in this object."""
        if not hasattr(self, "ready_pods"):
            raise NotImplementedError(f"{self.kind} does not support exec")
        pods: list[Pod] = self.ready_pods()
        if not pods:
            raise RuntimeError("No ready pods found")
        return pods[0].exec(
            command,
            container=container,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            check=check,
            capture_output=capture_output,
            timeout=timeout,
        )

    def watch(self) -> Generator[tuple[str, Self]]:
        """Watch this object in Kubernetes."""
        yield from self._watch()

    def _watch(self, timeout: float | None = None) -> Generator[tuple[str, Self]]:
        since = self.metadata.get("resourceVersion")
        assert self.api
        for event, obj in self.api.watch(
            self.endpoint,
            namespace=self.namespace,
            field_selector=f"metadata.name={self.name}",
            since=since,
            timeout=timeout,
        ):
            self.raw = obj.raw
            yield event, self

    def _sync_test_conditions(
        self, conditions: list, mode: Literal["any", "all"] = "any"
    ) -> bool:
        exists = self.exists() if "delete" in conditions else None
        return self._test_conditions(conditions, mode=mode, exists=exists)

    def wait(
        self,
        conditions: list[str] | str,
        mode: Literal["any", "all"] = "any",
        timeout: int | float | None = None,
    ):
        """Wait for conditions to be met.

        Args:
            conditions: A list of conditions to wait for.
            mode: Match any condition with "any" or all conditions with "all". Defaults to "any".
            timeout: Timeout in seconds.

        Raises:
            TimeoutError: If the conditions are not met before the timeout.

        Example:
            Wait for a Pod to be ready.

            >>> from kr8s.objects import Pod
            >>> pod = Pod.get("my-pod")
            >>> pod.wait("condition=Ready")

            Wait for a Job to either succeed or fail.

            >>> from kr8s.objects import Job
            >>> job = Job.get("my-jod")
            >>> job.wait(["condition=Complete", "condition=Failed"])

            Wait for a Pod to be initialized and ready to start containers.

            >>> from kr8s.objects import Pod
            >>> pod = Pod.get("my-pod")
            >>> pod.wait(["condition=Initialized", "condition=PodReadyToStartContainers"], mode="all")
        """
        if isinstance(conditions, str):
            conditions = [conditions]
        deadline = None if timeout is None else time.monotonic() + timeout

        def remaining() -> float | None:
            if deadline is None:
                return None
            left = deadline - time.monotonic()
            if left <= 0:
                raise TimeoutError
            return left

        try:
            try:
                self.refresh()
            except NotFoundError:
                if set(conditions) == {"delete"}:
                    return
            if self._sync_test_conditions(conditions, mode=mode):
                return
            for _ in self._watch(timeout=remaining()):
                if self._sync_test_conditions(conditions, mode=mode):
                    return
                remaining()
        except APITimeoutError as e:
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError from e
            raise
        # The watch ended without the conditions being met
        remaining()

    def annotate(self, annotations: dict | None = None, **kwargs) -> None:
        """Annotate this object in Kubernetes."""
        self.patch(self._annotations_patch(annotations, kwargs))

    def label(self, *labels: dict | str, **kwargs) -> None:
        """Add labels to this object in Kubernetes.

        Labels can be passed as a dictionary or as keyword arguments.

        Args:
            labels:
                A dictionary of labels to set or string to remove.
            **kwargs:
                Labels to set.

        Example:
            >>> from kr8s.objects import Deployment
            >>> deployment = Deployment.get("my-deployment")
            >>> # Both of these are equivalent
            >>> deployment.label({"app": "my-app"})
            >>> deployment.label(app="my-app")

            >>> # You can also remove a label by passing it's name with a `-` on the end
            >>> deployment.label("app-")
        """
        if (labels_to_remove := self._labels_to_remove(labels)) is not None:
            self.remove_label(*labels_to_remove)
        else:
            self.patch(self._labels_patch(labels, kwargs))

    def remove_label(self, *labels: str) -> None:
        """Remove labels from this object in Kubernetes.

        Labels can be passed as a list of strings.

        Args:
            labels: A list of labels to remove.

        Example:
            >>> from kr8s.objects import Deployment
            >>> deployment = Deployment.get("my-deployment")
            >>> deployment.remove_label("app")
        """
        self.patch(self._remove_labels_patch(labels), type="json")

    def set_owner(self, owner: BaseAPIObject) -> None:
        """Set the owner reference of this object.

        See https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/

        Args:
            owner: The owner object to set a reference to.

        Example:
            >>> from kr8s.objects import Deployment, Pod
            >>> deployment = Deployment.get("my-deployment")
            >>> pod = Pod.get("my-pod")
            >>> pod.set_owner(deployment)
        """
        self.patch(self._owner_patch(owner))

    def adopt(self, child: APIObject) -> None:
        """Adopt this object.

        This will set the owner reference of the child object to this object.

        See https://kubernetes.io/docs/concepts/overview/working-with-objects/owners-dependents/

        Args:
            child: The child object to adopt.

        Example:
            >>> from kr8s.objects import Deployment, Pod
            >>> deployment = Deployment.get("my-deployment")
            >>> pod = Pod.get("my-pod")
            >>> deployment.adopt(pod)

        """
        child.set_owner(self)

    # Must be the last method defined due to https://github.com/python/mypy/issues/17517
    @classmethod
    def list(cls, api: Api | None = None, **kwargs) -> Generator[Self | dict]:
        """List objects in Kubernetes.

        Args:
            api: An optional API object to use.
            **kwargs: Keyword arguments to pass to :func:`kr8s.get`.

        Returns:
            A list of objects or dictionaries (if raw=True).
        """
        if api is None:
            api = _api()
        for resource in api.get(kind=cls, **kwargs):
            if isinstance(resource, dict) or isinstance(resource, cls):
                yield resource


## v1 objects


class Binding(BaseBinding, APIObject):
    """A Kubernetes Binding."""


class ComponentStatus(BaseComponentStatus, APIObject):
    """A Kubernetes ComponentStatus."""


class ConfigMap(BaseConfigMap, APIObject):
    """A Kubernetes ConfigMap."""


class Endpoints(BaseEndpoints, APIObject):
    """A Kubernetes Endpoints."""


class Event(BaseEvent, APIObject):
    """A Kubernetes Event."""


class LimitRange(BaseLimitRange, APIObject):
    """A Kubernetes LimitRange."""


class Namespace(BaseNamespace, APIObject):
    """A Kubernetes Namespace."""


class Node(BaseNode, APIObject):
    """A Kubernetes Node."""

    def cordon(self) -> None:
        """Cordon the node.

        This will mark the node as unschedulable.
        """
        self.patch({"spec": {"unschedulable": True}})

    def uncordon(self) -> None:
        """Uncordon the node.

        This will mark the node as schedulable.
        """
        self.patch({"spec": {"unschedulable": False}})

    def taint(self, key: str, value: str, *, effect: str) -> None:
        """Taint a node."""
        self.refresh()
        self.patch(self._taints_patch(key, value, effect))


class PersistentVolumeClaim(BasePersistentVolumeClaim, APIObject):
    """A Kubernetes PersistentVolumeClaim."""


class PersistentVolume(BasePersistentVolume, APIObject):
    """A Kubernetes PersistentVolume."""


class Pod(BasePod, APIObject):
    """A Kubernetes Pod."""

    def ready(self) -> bool:
        """Check if the pod is ready."""
        self.refresh()
        return self._is_ready()

    def logs(
        self,
        container=None,
        pretty=None,
        previous=False,
        since_seconds=None,
        since_time=None,
        timestamps=False,
        tail_lines=None,
        limit_bytes=None,
        follow=False,
        timeout=3600,
    ) -> Generator[str]:
        """Streams logs from a Pod.

        Args:
            container:
                The container to get logs from. Defaults to the first container in the Pod.
            pretty:
                If True, return pretty logs. Defaults to False.
            previous:
                If True, return previous terminated container logs. Defaults to False.
            since_seconds:
                If set, return logs since this many seconds ago.
            since_time:
                If set, return logs since this time.
            timestamps:
                If True, prepend each log line with a timestamp. Defaults to False.
            tail_lines:
                If set, return this many lines from the end of the logs.
            limit_bytes:
                If set, return this many bytes from the end of the logs.
            follow:
                If True, follow the logs until the timeout is reached. Defaults to False.
            timeout:
                If following timeout after this many seconds. Set to None to disable timeout.

        Returns:
            A generator yielding log lines.


        Example:
            >>> from kr8s.objects import Pod
            >>> pod = Pod.get("my-pod")
            >>> for line in pod.logs():
            ...     print(line)

            We can also follow logs as they are generated, the generator will yield a new log line as
            it is generated by the Pod. This blocks indefinitely so we can set a timeout to break
            after some period of time, the default is ``3600`` (1hr) but can be set to ``None`` to
            disable the timeout.

            >>> from kr8s.objects import Pod
            >>> pod = Pod.get("my-pod", namespace="ns")
            >>> for line in pod.logs(follow=True, timeout=60):
            ...     # Will continue streaming logs until 60 seconds or the Pod is terminated
            ...     print(line)

        """
        params = self._logs_params(
            container=container,
            pretty=pretty,
            previous=previous,
            since_seconds=since_seconds,
            since_time=since_time,
            timestamps=timestamps,
            tail_lines=tail_lines,
            limit_bytes=limit_bytes,
            follow=follow,
        )
        with contextlib.suppress(httpx2.ReadTimeout):
            assert self.api
            with self.api.call_api(
                "GET",
                version=self.version,
                url=f"{self._url}/log",
                namespace=self.namespace,
                params=params,
                stream=True,
                timeout=timeout,
            ) as resp:
                yield from resp.iter_lines()

    def portforward(
        self,
        remote_port: int,
        local_port: LocalPortType = "match",
        address: list[str] | str = "127.0.0.1",
    ) -> PortForward:
        """Port forward a pod.

        Returns an instance of :class:`kr8s.portforward.PortForward` for this Pod.

        Args:
            remote_port:
                The port on the Pod to forward to.
            local_port:
                The local port to listen on. Defaults to ``"match"``, which will match the ``remote_port``.
                Set to ``"auto"`` or ``None`` to find an available high port.
                Set to an ``int`` to specify a specific port.
            address:
                List of addresses or address to listen on. Defaults to ["127.0.0.1"], will listen only on 127.0.0.1.

        Example:
            This can be used as a context manager or with explicit start/stop methods.

            Context manager:

            >>> with pod.portforward(8888) as port:
            ...     print(f"Forwarding to port {port}")
            ...     # Do something with port 8888


            Explict start/stop:

            >>> pf = pod.portforward(8888)
            >>> pf.start()
            >>> print(f"Forwarding to port {pf.local_port}")
            >>> # Do something with port 8888
            >>> pf.stop()

        """
        return PortForward(self, remote_port, local_port, address)

    def exec(
        self,
        command: list[str],
        *,
        container: str | None = None,
        stdin: str | BinaryIO | None = None,
        stdout: BinaryIO | None = None,
        stderr: BinaryIO | None = None,
        check: bool = True,
        capture_output: bool = True,
        timeout: int | None = None,
    ) -> CompletedExec:
        """Run a command in a container and wait until it completes.

        Behaves like :func:`subprocess.run`.

        Args:
            command:
                Command to execute.
            container:
                Container to execute the command in.
            stdin:
                If set, read stdin to the container.
            stdout:
                If set, write stdout to the provided writable stream object.
            stderr:
                If set, write stderr to the provided writable stream object.
            check:
                If True, raise an exception if the command fails.
            capture_output:
                If True, store stdout and stderr from the container in an attribute.
            timeout:
                If set, timeout for recieving bytes.

        Returns:
            A :class:`kr8s._exec.CompletedExec` object.

        Example:
            >>> from kr8s.objects import Pod
            >>> pod = Pod.get("my-pod")
            >>> ex = pod.exec(["ls", "-l"])
            >>> print(ex.stdout)
            >>> print(ex.stderr)
        """
        while not self.ready():
            time.sleep(0.1)

        ex = Exec(
            self,
            command,
            container=container,
            stdout=stdout,
            stderr=stderr,
            stdin=stdin,
            check=check,
            capture_output=capture_output,
            timeout=timeout,
        )
        with ex.run() as process:
            return process.wait()

    def tolerate(
        self,
        key: str,
        *,
        operator: str,
        effect: str,
        value: str | None = None,
        toleration_seconds: int | None = None,
    ):
        """Add a toleration to the Pod.

        Args:
            key (str): Key of the taint to tolerate.
            operator (str): Toleration operator.
            effect (str): Specifies the effect of the taint to tolerate.
            value (str): Value of taint to tolerate.
            toleration_seconds (str): Toleration seconds.
        """
        self.patch(
            self._tolerations_patch(key, operator, effect, value, toleration_seconds)
        )


class PodTemplate(BasePodTemplate, APIObject):
    """A Kubernetes PodTemplate."""


class ReplicationController(BaseReplicationController, APIObject):
    """A Kubernetes ReplicationController."""

    def ready(self):
        """Check if the deployment is ready."""
        self.refresh()
        return self._is_ready()


class ResourceQuota(BaseResourceQuota, APIObject):
    """A Kubernetes ResourceQuota."""


class Secret(BaseSecret, APIObject):
    """A Kubernetes Secret."""


class ServiceAccount(BaseServiceAccount, APIObject):
    """A Kubernetes ServiceAccount."""


class Service(BaseService, APIObject):
    """A Kubernetes Service."""

    def proxy_http_request(
        self, method: str, path: str, port: int | None = None, **kwargs: Any
    ) -> httpx2.Response:
        """Issue a HTTP request with specific HTTP method to proxy of a Service.

        Args:
            method: HTTP method to use.
            path: Path to proxy.
            port: Port to proxy to. If not specified, the first port in the
                Service's spec will be used.
            **kwargs: Additional keyword arguments to pass to the API call.
        """
        assert self.api
        with self.api.call_api(
            method,
            version=self.version,
            url=self._proxy_url(path, port),
            namespace=self.namespace,
            **kwargs,
        ) as response:
            return response

    def proxy_http_get(
        self, path: str, port: int | None = None, **kwargs
    ) -> httpx2.Response:
        return self.proxy_http_request("GET", path, port, **kwargs)

    def proxy_http_post(
        self, path: str, port: int | None = None, **kwargs
    ) -> httpx2.Response:
        return self.proxy_http_request("POST", path, port, **kwargs)

    def proxy_http_put(
        self, path: str, port: int | None = None, **kwargs
    ) -> httpx2.Response:
        return self.proxy_http_request("PUT", path, port, **kwargs)

    def proxy_http_delete(
        self, path: str, port: int | None = None, **kwargs
    ) -> httpx2.Response:
        return self.proxy_http_request("DELETE", path, port, **kwargs)

    def ready_pods(self) -> list[Pod]:
        """Return a list of ready Pods for this Service."""
        assert self.api
        pods = list(
            self.api.get(
                "pods",
                label_selector=self._pod_selector,
                namespace=self.namespace,
            )
        )
        if not all(isinstance(pod, Pod) for pod in pods):
            raise TypeError(f"Unexpected type {type(pods)} returned from API")
        # The all(isinstance(...) for ...) check doesn't narrow the type so we cast
        return [pod for pod in cast(list[Pod], pods) if pod.ready()]

    def ready(self) -> bool:
        """Check if the service is ready."""
        self.refresh()
        if not self._has_load_balancer_ingress():
            return False
        # Check there is at least one Pod in service
        return len(self.ready_pods()) > 0

    def portforward(
        self,
        remote_port: int,
        local_port: LocalPortType = "match",
        address: str | list[str] = "127.0.0.1",
    ) -> PortForward:
        """Port forward a service.

        Returns an instance of :class:`kr8s.portforward.PortForward` for this Service.

        Args:
            remote_port:
                The port on the Pod to forward to.
            local_port:
                The local port to listen on. Defaults to ``"match"``, which will match the ``remote_port``.
                Set to ``"auto"`` or ``None`` to find an available high port.
                Set to an ``int`` to specify a specific port.
            address:
                List of addresses or address to listen on. Defaults to ["127.0.0.1"], will listen only on 127.0.0.1.

        Example:
            This can be used as a context manager or with explicit start/stop methods.

            Context manager:

            >>> with service.portforward(8888) as port:
            ...     print(f"Forwarding to port {port}")
            ...     # Do something with port 8888


            Explict start/stop:

            >>> pf = service.portforward(8888)
            >>> pf.start()
            >>> print(f"Forwarding to port {pf.local_port}")
            >>> # Do something with port 8888
            >>> pf.stop()

        """
        return PortForward(self, remote_port, local_port, address)


## apps/v1 objects


class ControllerRevision(BaseControllerRevision, APIObject):
    """A Kubernetes ControllerRevision."""


class DaemonSet(BaseDaemonSet, APIObject):
    """A Kubernetes DaemonSet."""


class Deployment(BaseDeployment, APIObject):
    """A Kubernetes Deployment."""

    def ready_pods(self) -> list[Pod]:
        """Return a list of ready Pods for this Deployment."""
        return self.pods(ready=True)

    def pods(self, ready: bool = False) -> list[Pod]:
        """Return a list of Pods for this Deployment."""
        assert self.api
        pods = list(
            self.api.get(
                "pods",
                label_selector=self._pod_selector,
                namespace=self.namespace,
            )
        )
        if not all(isinstance(pod, Pod) for pod in pods):
            raise TypeError(f"Unexpected type {type(pods)} returned from API")
        # The all(isinstance(...) for ...) check doesn't narrow the type so we cast
        typed_pods = cast(list[Pod], pods)
        if ready:
            return [pod for pod in typed_pods if pod.ready()]
        return typed_pods

    def ready(self) -> bool:
        """Check if the deployment is ready."""
        self.refresh()
        return self._is_ready()


class ReplicaSet(BaseReplicaSet, APIObject):
    """A Kubernetes ReplicaSet."""


class StatefulSet(BaseStatefulSet, APIObject):
    """A Kubernetes StatefulSet."""


## autoscaling/v1 objects


class HorizontalPodAutoscaler(BaseHorizontalPodAutoscaler, APIObject):
    """A Kubernetes HorizontalPodAutoscaler."""


## batch/v1 objects


class CronJob(BaseCronJob, APIObject):
    """A Kubernetes CronJob."""


class Job(BaseJob, APIObject):
    """A Kubernetes Job."""


## networking.k8s.io/v1 objects


class IngressClass(BaseIngressClass, APIObject):
    """A Kubernetes IngressClass."""


class Ingress(BaseIngress, APIObject):
    """A Kubernetes Ingress."""


class NetworkPolicy(BaseNetworkPolicy, APIObject):
    """A Kubernetes NetworkPolicy."""


class IPAddress(BaseIPAddress, APIObject):
    """A Kubernetes IPAddress."""


class ServiceCIDR(BaseServiceCIDR, APIObject):
    """A Kubernetes ServiceCIDR."""


## policy/v1 objects


class PodDisruptionBudget(BasePodDisruptionBudget, APIObject):
    """A Kubernetes PodDisruptionBudget."""


## rbac.authorization.k8s.io/v1 objects


class ClusterRoleBinding(BaseClusterRoleBinding, APIObject):
    """A Kubernetes ClusterRoleBinding."""


class ClusterRole(BaseClusterRole, APIObject):
    """A Kubernetes ClusterRole."""


class RoleBinding(BaseRoleBinding, APIObject):
    """A Kubernetes RoleBinding."""


class Role(BaseRole, APIObject):
    """A Kubernetes Role."""


## apiextensions.k8s.io/v1 objects


class CustomResourceDefinition(BaseCustomResourceDefinition, APIObject):
    """A Kubernetes CustomResourceDefinition."""


## meta.k8s.io/v1 objects


class Table(BaseTable, APIObject):
    """A Kubernetes Table."""


def new_class(
    kind: str,
    version: str | None = None,
    asyncio: bool = False,
    namespaced=True,
    scalable: bool | None = None,
    scalable_spec: str | None = None,
    plural: str | None = None,
) -> type[APIObject]:
    """Create a new APIObject subclass.

    Args:
        kind: The Kubernetes resource kind.
        version: The Kubernetes API version.
        asyncio: Whether to use asyncio or not.
        namespaced: Whether the resource is namespaced or not.
        scalable: Whether the resource is scalable or not.
        scalable_spec: The name of the field to use for scaling.
        plural: The plural form of the resource.

    Returns:
        A new APIObject subclass.
    """
    return cast(
        type[APIObject],
        _new_class(kind, version, asyncio, namespaced, scalable, scalable_spec, plural),
    )


def object_from_name_type(
    name: str,
    namespace: str | None = None,
    api: Api | None = None,
) -> APIObject:
    """Create an APIObject from a Kubernetes resource name.

    Args:
        name: A Kubernetes resource name.
        namespace: The namespace of the resource.
        api: An optional API instance to use.

    Returns:
        A corresponding APIObject subclass instance.

    Raises:
        ValueError: If the resource kind or API version is not supported.
    """
    name, kind, version = parse_name_type(name)
    return get_class(kind, version).get(name, namespace=namespace, api=api)


def objects_from_files(
    path,
    api: Api | None = None,
    recursive: bool = False,
) -> list[APIObject]:
    """Create APIObjects from Kubernetes resource files.

    Args:
        path: A path to a Kubernetes resource file or directory of resource files.
        api: An optional API instance to use.
        recursive: Whether to recursively search for resource files in subdirectories.

    Returns:
        A list of APIObject subclass instances.

    Raises:
        ValueError: If the resource kind or API version is not supported.
    """
    files = find_object_files(path, recursive)
    if not api:
        api = _api()
    objects = []
    for file in files:
        with open(file) as f:
            for doc in yaml.safe_load_all(f):
                if doc is not None:
                    objects.append(
                        object_from_spec(doc, api=api, allow_unknown_type=True)
                    )
    return objects


def get_class(kind: str, version: str | None = None) -> type[APIObject]:
    """Get an APIObject subclass by kind and version.

    Args:
        kind: The Kubernetes resource kind.
        version: The Kubernetes API group/version.

    Returns:
        An APIObject subclass.

    Raises:
        KeyError: If no object is registered for the given kind and version.
    """
    return cast(type[APIObject], _get_class(kind, version, _asyncio=False))


def object_from_spec(
    spec: dict, api: Api | None = None, allow_unknown_type: bool = False
) -> APIObject:
    """Create an APIObject from a Kubernetes resource spec.

    Args:
        spec: A Kubernetes resource spec.
        api: An optional API instance to use.
        allow_unknown_type: Whether to allow unknown resource types.

    Returns:
        A corresponding APIObject subclass instance.

    Raises:
        ValueError: If the resource kind or API version is not supported.
    """
    return cast(
        APIObject, _object_from_spec(spec, api, allow_unknown_type, _asyncio=False)
    )
