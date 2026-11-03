# SPDX-FileCopyrightText: Copyright (c) 2023-2026, Kr8s Developers (See LICENSE for list)
# SPDX-License-Identifier: BSD 3-Clause License
"""Base classes for Kubernetes objects.

Each Kubernetes kind has a ``Base<Kind>`` class here which holds its attributes and every piece
of logic that doesn't need I/O. The sync classes in :mod:`kr8s.objects` and the async classes in
:mod:`kr8s.asyncio.objects` combine these with their own ``APIObject``, which implements the API calls.
"""

from __future__ import annotations

import contextlib
import json
import pathlib
import re
from typing import (
    TYPE_CHECKING,
    Any,
    Literal,
)

import jsonpath
import yaml
from box import Box

from kr8s._data_utils import (
    dict_to_selector,
    dot_to_nested_dict,
    escape_rfc6901,
    list_dict_unpack,
    xdict,
)
from kr8s._exceptions import NotFoundError, ServerError
from kr8s._types import SpecType, SupportsKeysAndGetItem

if TYPE_CHECKING:
    from kr8s._api import BaseApi

JSONPATH_CONDITION_EXPRESSION = r"jsonpath='{(?P<expression>.*?)}'=(?P<condition>.*)"


class BaseAPIObject:
    """Base class for Kubernetes objects."""

    version: str
    endpoint: str
    kind: str
    plural: str
    singular: str
    namespaced: bool = False
    scalable: bool = False
    scalable_spec: str = "replicas"
    # True for async objects, False for sync objects and None for the shared base classes
    _asyncio: bool | None = None

    def __init__(
        self,
        resource: SpecType,
        namespace: str | None = None,
        api: BaseApi | None = None,
    ) -> None:
        """Initialize an APIObject."""
        if isinstance(resource, dict):
            self.raw = resource
        elif isinstance(resource, SupportsKeysAndGetItem):
            self.raw = dict(resource)
        elif isinstance(resource, str):
            self.raw = {"metadata": {"name": resource}}
        elif hasattr(resource, "to_dict"):
            self.raw = resource.to_dict()
        elif hasattr(resource, "obj") and isinstance(resource.obj, dict):
            self.raw = resource.obj
        else:
            raise ValueError(
                "resource must be a dict, string, have a to_dict method or an obj attribute containing a dict"
            )
        if namespace is not None:
            self.raw["metadata"]["namespace"] = namespace  # type: ignore
        self._api = api

    def __repr__(self):
        """Return a string representation of the Kubernetes resource."""
        if "name" in self.metadata:
            return f"<{self.kind} {self.name}>"
        if "generateName" in self.metadata:
            return f"<{self.kind} generateName({self.metadata.generateName})>"
        return f"<{self.kind} UNKNOWN>"

    def __str__(self):
        """Return a string representation of the Kubernetes resource."""
        return self.name

    def __eq__(self, other):
        if self.version != other.version:
            return False
        if self.kind != other.kind:
            return False
        if self.name != other.name:
            return False
        if self.namespaced and self.namespace != other.namespace:
            return False
        return True

    @property
    def api(self):
        if self._api is None and self._asyncio:
            raise RuntimeError(
                f"{self.__repr__()} has not been initialized with an API object. "
                "Did you forget to await it?"
            )
        return self._api

    @api.setter
    def api(self, value):
        self._api = value

    @property
    def raw(self) -> Box:
        """Raw object returned from the Kubernetes API."""
        self._raw.update({"kind": self.kind, "apiVersion": self.version})
        return self._raw

    @raw.setter
    def raw(self, value: Any) -> None:
        self._raw = Box(value)

    @property
    def raw_template(self) -> Box:
        """Template representation of the Kubernetes resource without instance-specific data.

        Returns a copy of the raw object with metadata.resourceVersion and status removed.
        This is useful for creating new resources based on existing ones.
        """
        template = self.raw.copy()
        if "metadata" in template:
            template.metadata.pop("resourceVersion", None)
        return template

    @property
    def name(self) -> str:
        """Name of the Kubernetes resource."""
        if "name" in self.metadata:
            return self.metadata.name
        if "generateName" in self.metadata:
            raise ValueError("Resource has a generateName that has not been resolved")
        raise ValueError("Resource does not have a name")

    @name.setter
    def name(self, value: str) -> None:
        self.raw["metadata"]["name"] = value

    @property
    def namespace(self) -> str | None:
        """Namespace of the Kubernetes resource."""
        if self.namespaced:
            assert self.api
            return self.raw.get("metadata", {}).get("namespace", self.api.namespace)
        return None

    @namespace.setter
    def namespace(self, value: str) -> None:
        if self.namespaced:
            self.raw["metadata"]["namespace"] = value

    @property
    def metadata(self) -> Box:
        """Metadata of the Kubernetes resource."""
        return self.raw["metadata"]

    @metadata.setter
    def metadata(self, value: dict) -> None:
        self.raw["metadata"] = value

    @property
    def spec(self) -> Box:
        """Spec of the Kubernetes resource."""
        return self.raw["spec"]

    @spec.setter
    def spec(self, value: dict) -> None:
        self.raw["spec"] = value

    @property
    def status(self) -> Box:
        """Status of the Kubernetes resource."""
        return self.raw["status"]

    @status.setter
    def status(self, value: dict) -> None:
        self.raw["status"] = value

    @property
    def labels(self) -> Box:
        """Labels of the Kubernetes resource."""
        try:
            return self.raw["metadata"]["labels"]
        except KeyError:
            return Box({})

    @labels.setter
    def labels(self, value: dict) -> None:
        self.raw["metadata"]["labels"] = value

    @property
    def annotations(self) -> Box:
        """Annotations of the Kubernetes resource."""
        try:
            return self.raw["metadata"]["annotations"]
        except KeyError:
            return Box({})

    @annotations.setter
    def annotations(self, value: dict) -> None:
        self.raw["metadata"]["annotations"] = value

    @property
    def replicas(self) -> int:
        """Replicas of the Kubernetes resource."""
        if self.scalable:
            keys = self.scalable_spec.split(".")
            spec = self.raw["spec"]
            for key in keys:
                spec = spec[key]
            return spec
        raise NotImplementedError(f"{self.kind} is not scalable")

    @replicas.setter
    def replicas(self, value: int) -> None:
        if self.scalable:
            raise NotImplementedError(
                f"Use scale() method to set replicas for {self.kind}"
            )
        else:
            raise NotImplementedError(f"{self.kind} is not scalable")

    @property
    def _url(self) -> str:
        return f"{self.endpoint}/{self.name}"

    def _not_found(self, e: ServerError) -> NotFoundError | None:
        """Convert a 404 from the API into a NotFoundError."""
        if e.response and e.response.status_code == 404:
            return NotFoundError(
                f"Object {self.name} does not exist in {self.namespace}"
            )
        return None

    @staticmethod
    def _delete_payload(
        propagation_policy: str | None, grace_period: int | None, force: bool
    ) -> str:
        data: dict[str, Any] = {}
        if propagation_policy:
            data["propagationPolicy"] = propagation_policy
        if grace_period and force:
            raise ValueError("Cannot set both grace_period and force")
        if grace_period:
            data["gracePeriodSeconds"] = grace_period
        elif force:
            data["gracePeriodSeconds"] = 0
        return json.dumps(data)

    def _patch_request(
        self, subresource: str | None, type: str | None
    ) -> tuple[str, dict[str, str]]:
        """Return the URL and headers for a patch."""
        url = self._url
        if type == "json":
            headers = {"Content-Type": "application/json-patch+json"}
        else:
            headers = {"Content-Type": "application/merge-patch+json"}
        if subresource:
            url = f"{url}/{subresource}"
        return url, headers

    def _scale_patch(self, replicas: int | None) -> dict:
        if not self.scalable:
            raise NotImplementedError(f"{self.kind} is not scalable")
        return {"spec": dot_to_nested_dict(self.scalable_spec, replicas)}

    @staticmethod
    def _annotations_patch(annotations: dict | None, kwargs: dict) -> dict:
        if annotations is None:
            annotations = kwargs
        if not annotations:
            raise ValueError("No annotations provided")
        return {"metadata": {"annotations": annotations}}

    @staticmethod
    def _labels_to_remove(labels: tuple[dict | str, ...]) -> list[str] | None:
        """Return the label names to remove if ``labels`` are all ``name-`` strings."""
        if labels and all([isinstance(label, str) for label in labels]):
            labels_to_remove = []
            for label in labels:
                assert isinstance(label, str)
                assert label.endswith("-"), "Label must end with - to remove"
                labels_to_remove.append(label[:-1])
            return labels_to_remove
        return None

    @staticmethod
    def _labels_patch(labels: tuple[dict | str, ...], kwargs: dict) -> dict:
        labels_dict = None
        if not labels:
            labels_dict = kwargs
        if labels and isinstance(labels[0], dict):
            labels_dict = labels[0]
        if not labels_dict:
            raise ValueError("No labels provided")
        return {"metadata": {"labels": labels_dict}}

    @staticmethod
    def _remove_labels_patch(labels: tuple[str, ...]) -> list[dict]:
        if not labels:
            raise ValueError("No labels provided")
        return [
            {"op": "remove", "path": "/metadata/labels/" + escape_rfc6901(label)}
            for label in labels
        ]

    @staticmethod
    def _owner_patch(owner: BaseAPIObject) -> dict:
        return {
            "metadata": {
                "ownerReferences": [
                    {
                        "controller": True,
                        "blockOwnerDeletion": True,
                        "apiVersion": owner.version,
                        "kind": owner.kind,
                        "name": owner.name,
                        "uid": owner.metadata.uid,
                    }
                ],
            }
        }

    def _test_conditions(
        self,
        conditions: list,
        mode: Literal["any", "all"] = "any",
        exists: bool | None = None,
    ) -> bool:
        """Test if conditions are met.

        Args:
            conditions: A list of conditions to test.
            mode: Match any condition with "any" or all conditions with "all". Defaults to "any".
            exists: Whether the object still exists, required to test the "delete" condition.

        Returns:
            bool: True if any condition is met, False otherwise.
        """
        results = []
        for condition in conditions:
            if condition.startswith("condition"):
                condition = "=".join(condition.split("=")[1:])
                if "=" in condition:
                    field, value = condition.split("=")
                    value = str(value)
                else:
                    field = condition
                    value = str(True)
                if value == "true" or value == "false":
                    value = value.title()
                status_conditions = list_dict_unpack(
                    self.status.get("conditions", []), "type", "status"
                )
                results.append(status_conditions.get(field, None) == value)
            elif condition == "delete":
                results.append(not exists)
            elif condition.startswith("jsonpath"):
                matches = re.search(JSONPATH_CONDITION_EXPRESSION, condition)
                if not matches:
                    raise ValueError(
                        f"Invalid jsonpath condition expression '{condition}'"
                    )
                expression = matches.group("expression")
                condition = matches.group("condition")
                [value] = [str(v) for v in jsonpath.findall(expression, self.raw)]
                results.append(str(value) == str(condition))
            else:
                raise ValueError(f"Unknown condition type {condition}")
        if mode == "any":
            return any(results)
        elif mode == "all":
            return all(results)
        else:
            raise ValueError(f"Unknown wait mode '{mode}', must be 'any' or 'all'")

    def keys(self) -> list:
        """Return the keys of this object."""
        return self.raw.keys()

    def __getitem__(self, key: str) -> Any:
        """Get an item from this object."""
        return self.raw[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Set an item in this object."""
        self.raw[key] = value

    def to_dict(self) -> dict:
        """Return a dictionary representation of this object."""
        return self.raw.to_dict()

    def to_yaml(self) -> str:
        """Return a YAML representation of this object."""
        return yaml.dump(self.to_dict())

    def to_lightkube(self) -> Any:
        """Return a lightkube representation of this object."""
        try:
            from lightkube import codecs
        except ImportError as e:
            raise ImportError("lightkube is not installed") from e
        return codecs.from_dict(self.raw)

    def to_pykube(self, api) -> Any:
        """Return a pykube representation of this object.

        Args:
            api: A pykube API object.

        Example:
            >>> from kr8s.objects import Deployment
            >>> deployment = Deployment.get("my-deployment")
            >>> # Create a pykube API object
            >>> from pykube import HTTPClient
            >>> api = HTTPClient()
            >>> pykube_deployment = deployment.to_pykube(api)

        """
        try:
            import pykube  # type: ignore
        except ImportError as e:
            raise ImportError("pykube is not installed") from e
        try:
            pykube_cls = getattr(pykube.objects, self.kind)
        except AttributeError:
            base = (
                pykube.objects.NamespacedAPIObject
                if self.namespaced
                else pykube.objects.APIObject
            )
            pykube_cls = type(
                self.kind,
                (base,),
                {"version": self.version, "endpoint": self.endpoint, "kind": self.kind},
            )
        return pykube_cls(api, self.raw)

    def pprint(self, use_rich: bool = True, theme: str = "ansi_dark") -> None:
        """Pretty print this object to stdout.

        Prints the object as YAML (using ``rich`` for syntax highlighting if available).

        Args:
            use_rich: Use ``rich`` to pretty print. If ``rich` is not installed this will be ignored.
            theme: The ``pygments`` theme for ``rich`` to use. Defaults to "ansi_dark" to use default terminal colors.

        """
        if use_rich:
            with contextlib.suppress(ImportError):
                from rich import print as rich_print
                from rich.syntax import Syntax

                rich_print(Syntax(code=self.to_yaml(), lexer="yaml", theme=theme))
                return
        print(self.to_yaml())

    @classmethod
    def gen(cls, *args, **kwargs):
        raise NotImplementedError("gen is not implemented for this object")


## v1 objects


class BaseBinding(BaseAPIObject):
    """A Kubernetes Binding."""

    version = "v1"
    endpoint = "bindings"
    kind = "Binding"
    plural = "bindings"
    singular = "binding"
    namespaced = True


class BaseComponentStatus(BaseAPIObject):
    """A Kubernetes ComponentStatus."""

    version = "v1"
    endpoint = "componentstatuses"
    kind = "ComponentStatus"
    plural = "componentstatuses"
    singular = "componentstatus"
    namespaced = False


class BaseConfigMap(BaseAPIObject):
    """A Kubernetes ConfigMap."""

    version = "v1"
    endpoint = "configmaps"
    kind = "ConfigMap"
    plural = "configmaps"
    singular = "configmap"
    namespaced = True

    @property
    def data(self) -> Box:
        """Data of the ConfigMap."""
        return self.raw["data"]

    @data.setter
    def data(self, value: dict) -> None:
        self.raw["data"] = value


class BaseEndpoints(BaseAPIObject):
    """A Kubernetes Endpoints."""

    version = "v1"
    endpoint = "endpoints"
    kind = "Endpoints"
    plural = "endpoints"
    singular = "endpoint"
    namespaced = True


class BaseEvent(BaseAPIObject):
    """A Kubernetes Event."""

    version = "v1"
    endpoint = "events"
    kind = "Event"
    plural = "events"
    singular = "event"
    namespaced = True


class BaseLimitRange(BaseAPIObject):
    """A Kubernetes LimitRange."""

    version = "v1"
    endpoint = "limitranges"
    kind = "LimitRange"
    plural = "limitranges"
    singular = "limitrange"
    namespaced = True


class BaseNamespace(BaseAPIObject):
    """A Kubernetes Namespace."""

    version = "v1"
    endpoint = "namespaces"
    kind = "Namespace"
    plural = "namespaces"
    singular = "namespace"
    namespaced = False


class BaseNode(BaseAPIObject):
    """A Kubernetes Node."""

    version = "v1"
    endpoint = "nodes"
    kind = "Node"
    plural = "nodes"
    singular = "node"
    namespaced = False

    @property
    def unschedulable(self):
        if "unschedulable" in self.raw["spec"]:
            return self.raw["spec"]["unschedulable"]
        return False

    @property
    def taints(self) -> Box:
        """Labels of the Kubernetes resource."""
        try:
            return self.raw["spec"]["taints"]
        except KeyError:
            return Box({})

    def _taints_patch(self, key: str, value: str, effect: str) -> dict:
        if effect.endswith("-"):
            # Remove taint with key
            effect = effect[:-1]
            if all(taint["key"] != key for taint in self.taints):
                raise NotFoundError(f"Unable to find taint with key: {key}")

            taints = [taint for taint in self.taints if taint["key"] != key]
        else:
            taints = list(self.taints) + [
                {"key": key, "value": value, "effect": effect}
            ]
        return {"spec": {"taints": taints}}


class BasePersistentVolumeClaim(BaseAPIObject):
    """A Kubernetes PersistentVolumeClaim."""

    version = "v1"
    endpoint = "persistentvolumeclaims"
    kind = "PersistentVolumeClaim"
    plural = "persistentvolumeclaims"
    singular = "persistentvolumeclaim"
    namespaced = True


class BasePersistentVolume(BaseAPIObject):
    """A Kubernetes PersistentVolume."""

    version = "v1"
    endpoint = "persistentvolumes"
    kind = "PersistentVolume"
    plural = "persistentvolumes"
    singular = "persistentvolume"
    namespaced = False


class BasePod(BaseAPIObject):
    """A Kubernetes Pod."""

    version = "v1"
    endpoint = "pods"
    kind = "Pod"
    plural = "pods"
    singular = "pod"
    namespaced = True

    def _is_ready(self) -> bool:
        conditions = list_dict_unpack(
            self.status.get("conditions", []),
            key="type",
            value="status",
        )
        return (
            "Ready" in conditions
            and "ContainersReady" in conditions
            and conditions.get("Ready", "False") == "True"
            and conditions.get("ContainersReady", "False") == "True"
        )

    @staticmethod
    def _logs_params(
        container=None,
        pretty=None,
        previous=False,
        since_seconds=None,
        since_time=None,
        timestamps=False,
        tail_lines=None,
        limit_bytes=None,
        follow=False,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if follow:
            params["follow"] = "true"
        if container is not None:
            params["container"] = container
        if pretty is not None:
            params["pretty"] = pretty
        if previous:
            params["previous"] = "true"
        if since_seconds is not None and since_time is None:
            params["sinceSeconds"] = int(since_seconds)
        elif since_time is not None and since_seconds is None:
            params["sinceTime"] = since_time
        if timestamps:
            params["timestamps"] = "true"
        if tail_lines is not None:
            params["tailLines"] = int(tail_lines)
        if limit_bytes is not None:
            params["limitBytes"] = int(limit_bytes)
        return params

    @classmethod
    def gen(
        cls,
        *,
        name=None,
        generate_name=None,
        image=None,
        namespace=None,
        annotations=None,
        command=None,
        env=None,
        resources=None,
        image_pull_policy=None,
        labels=None,
        ports=None,
        restart="Always",
    ):
        """Generate a pod definition.

        Args:
            name (str): The name of the pod.
            generate_name (str): Template for generating the name of the pod.
            namespace (str): The namespace of the pod.
            image (str): The image to use.
            annotations (dict): Annotations to add to the pod.
            command (list): Command to run in the container.
            env (dict): Environment variables to set in the container.
            resources (dict): Resources to set in the container.
            image_pull_policy (str): Image pull policy to use.
            labels (dict): Labels to add to the pod.
            ports (list|int): Ports to expose.
            restart (str): Restart policy to use.

        Returns:
            A :class:`kr8s.objects.Pod` object.

        Example:
            >>> from kr8s.objects import Pod
            >>> pod = Pod.gen(name="my-pod", image="my-image")
            >>> pod.create()

            Create an nginx Pod that exposes port 80.
            >>> from kr8s.objects import Pod
            >>> pod = Pod.gen(name="nginx", image="nginx:latest", ports=[80])
            >>> pod.create()

            Create an wordpress Pod that exposes port 80.
            >>> from kr8s.objects import Pod
            >>> pod = Pod.gen(name="wordpress", image="wordpress:latest", ports=[{"containerPort": 80}])
            >>> pod.create()

            Create a Pod that requires a GPU
            >>> from kr8s.objects import Pod
            >>> pod = Pod.gen(name="cuda-vectoradd",
            ...               image="nvidia/samples:vectoradd-cuda11.6.0-ubuntu18.04",
            ...               resources={"limits": {"nvidia.com/gpu": 1}})
        """
        if not image:
            raise ValueError("Image must be specified")
        if not name and not generate_name:
            raise ValueError("Name or generate_name must be specified")
        if ports:
            if isinstance(ports, int):
                ports = [ports]
            # Convert any integer ports to valid v1.ContainerPort format
            ports = [
                {"containerPort": port} if isinstance(port, int) else port
                for port in ports
            ]

        return cls(
            xdict(
                apiVersion="v1",
                kind="Pod",
                metadata=xdict(
                    name=name,
                    generateName=generate_name,
                    namespace=namespace,
                    annotations=annotations,
                    labels=labels,
                ),
                spec=xdict(
                    containers=[
                        xdict(
                            name=name if name else "container",
                            image=image,
                            command=command,
                            env=env,
                            resources=resources,
                            imagePullPolicy=image_pull_policy,
                            ports=ports,
                        )
                    ],
                    restartPolicy=restart,
                ),
            )
        )

    def _tolerations_patch(
        self,
        key: str,
        operator: str,
        effect: str,
        value: str | None,
        toleration_seconds: int | None,
    ) -> dict:
        new_toleration: dict = {"key": key, "operator": operator, "effect": effect}
        if value is not None:
            new_toleration["value"] = value

        if toleration_seconds is not None:
            new_toleration["tolerationSeconds"] = toleration_seconds

        self.tolerations.append(new_toleration)
        return {"spec": {"tolerations": self.tolerations}}

    @property
    def tolerations(self) -> Box:
        """Tolerations for Pod."""
        try:
            return self.raw["spec"]["tolerations"]
        except KeyError:
            return Box([])


class BasePodTemplate(BaseAPIObject):
    """A Kubernetes PodTemplate."""

    version = "v1"
    endpoint = "podtemplates"
    kind = "PodTemplate"
    plural = "podtemplates"
    singular = "podtemplate"
    namespaced = True


class BaseReplicationController(BaseAPIObject):
    """A Kubernetes ReplicationController."""

    version = "v1"
    endpoint = "replicationcontrollers"
    kind = "ReplicationController"
    plural = "replicationcontrollers"
    singular = "replicationcontroller"
    namespaced = True
    scalable = True

    def _is_ready(self) -> bool:
        return (
            self.raw["status"].get("observedGeneration", 0)
            >= self.raw["metadata"]["generation"]
            and self.raw["status"].get("readyReplicas", 0) == self.replicas
        )


class BaseResourceQuota(BaseAPIObject):
    """A Kubernetes ResourceQuota."""

    version = "v1"
    endpoint = "resourcequotas"
    kind = "ResourceQuota"
    plural = "resourcequotas"
    singular = "resourcequota"
    namespaced = True


class BaseSecret(BaseAPIObject):
    """A Kubernetes Secret."""

    version = "v1"
    endpoint = "secrets"
    kind = "Secret"
    plural = "secrets"
    singular = "secret"
    namespaced = True

    @property
    def data(self) -> Box:
        """Data of the Secret."""
        return self.raw["data"]

    @data.setter
    def data(self, value: dict) -> None:
        self.raw["data"] = value


class BaseServiceAccount(BaseAPIObject):
    """A Kubernetes ServiceAccount."""

    version = "v1"
    endpoint = "serviceaccounts"
    kind = "ServiceAccount"
    plural = "serviceaccounts"
    singular = "serviceaccount"
    namespaced = True


class BaseService(BaseAPIObject):
    """A Kubernetes Service."""

    version = "v1"
    endpoint = "services"
    kind = "Service"
    plural = "services"
    singular = "service"
    namespaced = True

    def _proxy_url(self, path: str, port: int | None) -> str:
        if port is None:
            port = self.raw["spec"]["ports"][0]["port"]
        return f"{self.endpoint}/{self.name}:{port}/proxy/{path}"

    @property
    def _pod_selector(self) -> str:
        return dict_to_selector(self.spec["selector"])

    def _has_load_balancer_ingress(self) -> bool:
        """Whether the Service is ready to receive traffic from its load balancer, if it has one."""
        return not (
            self.spec.type == "LoadBalancer"
            and len(self.status.loadBalancer.ingress or []) == 0
        )


## apps/v1 objects


class BaseControllerRevision(BaseAPIObject):
    """A Kubernetes ControllerRevision."""

    version = "apps/v1"
    endpoint = "controllerrevisions"
    kind = "ControllerRevision"
    plural = "controllerrevisions"
    singular = "controllerrevision"
    namespaced = True


class BaseDaemonSet(BaseAPIObject):
    """A Kubernetes DaemonSet."""

    version = "apps/v1"
    endpoint = "daemonsets"
    kind = "DaemonSet"
    plural = "daemonsets"
    singular = "daemonset"
    namespaced = True


class BaseDeployment(BaseAPIObject):
    """A Kubernetes Deployment."""

    version = "apps/v1"
    endpoint = "deployments"
    kind = "Deployment"
    plural = "deployments"
    singular = "deployment"
    namespaced = True
    scalable = True

    @property
    def _pod_selector(self) -> str:
        return dict_to_selector(self.spec["selector"]["matchLabels"])

    def _is_ready(self) -> bool:
        return (
            self.raw["status"].get("observedGeneration", 0)
            >= self.raw["metadata"]["generation"]
            and self.raw["status"].get("readyReplicas", 0) == self.replicas
        )


class BaseReplicaSet(BaseAPIObject):
    """A Kubernetes ReplicaSet."""

    version = "apps/v1"
    endpoint = "replicasets"
    kind = "ReplicaSet"
    plural = "replicasets"
    singular = "replicaset"
    namespaced = True
    scalable = True


class BaseStatefulSet(BaseAPIObject):
    """A Kubernetes StatefulSet."""

    version = "apps/v1"
    endpoint = "statefulsets"
    kind = "StatefulSet"
    plural = "statefulsets"
    singular = "statefulset"
    namespaced = True
    scalable = True


## autoscaling/v1 objects


class BaseHorizontalPodAutoscaler(BaseAPIObject):
    """A Kubernetes HorizontalPodAutoscaler."""

    version = "autoscaling/v2"
    endpoint = "horizontalpodautoscalers"
    kind = "HorizontalPodAutoscaler"
    plural = "horizontalpodautoscalers"
    singular = "horizontalpodautoscaler"
    namespaced = True


## batch/v1 objects


class BaseCronJob(BaseAPIObject):
    """A Kubernetes CronJob."""

    version = "batch/v1"
    endpoint = "cronjobs"
    kind = "CronJob"
    plural = "cronjobs"
    singular = "cronjob"
    namespaced = True


class BaseJob(BaseAPIObject):
    """A Kubernetes Job."""

    version = "batch/v1"
    endpoint = "jobs"
    kind = "Job"
    plural = "jobs"
    singular = "job"
    namespaced = True
    scalable = True
    scalable_spec = "parallelism"


## networking.k8s.io/v1 objects


class BaseIngressClass(BaseAPIObject):
    """A Kubernetes IngressClass."""

    version = "networking.k8s.io/v1"
    endpoint = "ingressclasses"
    kind = "IngressClass"
    plural = "ingressclasses"
    singular = "ingressclass"
    namespaced = False


class BaseIngress(BaseAPIObject):
    """A Kubernetes Ingress."""

    version = "networking.k8s.io/v1"
    endpoint = "ingresses"
    kind = "Ingress"
    plural = "ingresses"
    singular = "ingress"
    namespaced = True


class BaseNetworkPolicy(BaseAPIObject):
    """A Kubernetes NetworkPolicy."""

    version = "networking.k8s.io/v1"
    endpoint = "networkpolicies"
    kind = "NetworkPolicy"
    plural = "networkpolicies"
    singular = "networkpolicy"
    namespaced = True


class BaseIPAddress(BaseAPIObject):
    """A Kubernetes IPAddress."""

    version = "networking.k8s.io/v1"
    endpoint = "ipaddresses"
    kind = "IPAddress"
    plural = "ipaddresses"
    singular = "ipaddress"
    namespaced = False


class BaseServiceCIDR(BaseAPIObject):
    """A Kubernetes ServiceCIDR."""

    version = "networking.k8s.io/v1"
    endpoint = "servicecidrs"
    kind = "ServiceCIDR"
    plural = "servicecidrs"
    singular = "servicecidr"
    namespaced = False


## policy/v1 objects


class BasePodDisruptionBudget(BaseAPIObject):
    """A Kubernetes PodDisruptionBudget."""

    version = "policy/v1"
    endpoint = "poddisruptionbudgets"
    kind = "PodDisruptionBudget"
    plural = "poddisruptionbudgets"
    singular = "poddisruptionbudget"
    namespaced = True


## rbac.authorization.k8s.io/v1 objects


class BaseClusterRoleBinding(BaseAPIObject):
    """A Kubernetes ClusterRoleBinding."""

    version = "rbac.authorization.k8s.io/v1"
    endpoint = "clusterrolebindings"
    kind = "ClusterRoleBinding"
    plural = "clusterrolebindings"
    singular = "clusterrolebinding"
    namespaced = False


class BaseClusterRole(BaseAPIObject):
    """A Kubernetes ClusterRole."""

    version = "rbac.authorization.k8s.io/v1"
    endpoint = "clusterroles"
    kind = "ClusterRole"
    plural = "clusterroles"
    singular = "clusterrole"
    namespaced = False


class BaseRoleBinding(BaseAPIObject):
    """A Kubernetes RoleBinding."""

    version = "rbac.authorization.k8s.io/v1"
    endpoint = "rolebindings"
    kind = "RoleBinding"
    plural = "rolebindings"
    singular = "rolebinding"
    namespaced = True


class BaseRole(BaseAPIObject):
    """A Kubernetes Role."""

    version = "rbac.authorization.k8s.io/v1"
    endpoint = "roles"
    kind = "Role"
    plural = "roles"
    singular = "role"
    namespaced = True


## apiextensions.k8s.io/v1 objects


class BaseCustomResourceDefinition(BaseAPIObject):
    """A Kubernetes CustomResourceDefinition."""

    version = "apiextensions.k8s.io/v1"
    endpoint = "customresourcedefinitions"
    kind = "CustomResourceDefinition"
    plural = "customresourcedefinitions"
    singular = "customresourcedefinition"
    namespaced = False


## meta.k8s.io/v1 objects


class BaseTable(BaseAPIObject):
    """A Kubernetes Table."""

    version = "meta.k8s.io/v1"
    endpoint = "tables"
    kind = "Table"
    plural = "tables"
    singular = "table"
    namespaced = False

    @property
    def rows(self) -> list[dict]:
        """Table rows."""
        return self.raw["rows"]

    @property
    def column_definitions(self) -> list[dict]:
        """Table column definitions."""
        return self.raw["columnDefinitions"]


def _api_object_class(asyncio: bool) -> type[BaseAPIObject]:
    """Return the sync or async ``APIObject`` that all objects of that flavour subclass."""
    if asyncio:
        from kr8s.asyncio.objects import APIObject as AsyncAPIObject

        return AsyncAPIObject
    from kr8s.objects import APIObject as SyncAPIObject

    return SyncAPIObject


def get_class(
    kind: str,
    version: str | None = None,
    _asyncio: bool = True,
) -> type[BaseAPIObject]:
    """Get an APIObject subclass by kind and version.

    Args:
        kind: The Kubernetes resource kind.
        version: The Kubernetes API group/version.

    Returns:
        An APIObject subclass.

    Raises:
        KeyError: If no object is registered for the given kind and version.
    """
    result = None
    group = None
    if "/" in kind:
        kind, version = kind.split("/", 1)
    if "." in kind:
        kind, group = kind.split(".", 1)
    if version and "/" in version:
        if group:
            raise ValueError("Cannot specify group in both kind and version")
        group, version = version.split("/", 1)
    kind = kind.lower()

    def _walk_subclasses(cls):
        yield cls
        for subcls in cls.__subclasses__():
            yield from _walk_subclasses(subcls)

    for cls in _walk_subclasses(_api_object_class(_asyncio)):
        if not hasattr(cls, "version"):
            continue
        if "/" in cls.version:
            cls_group, cls_version = cls.version.split("/")
        else:
            cls_group, cls_version = None, cls.version
        if hasattr(cls, "kind") and (
            cls.kind == kind or cls.singular == kind or cls.plural == kind
        ):
            if (group is None or cls_group == group) and (
                version is None or cls_version == version
            ):
                result = cls
            if (
                group
                and not version
                and "." in group
                and cls_group == group.split(".", 1)[1]
                and cls_version == group.split(".", 1)[0]
            ):
                result = cls

    if result:
        return result
    raise KeyError(
        f"No object registered for {kind}{'.' + group if group else ''}. "
        "See https://docs.kr8s.org/en/stable/object.html#extending-the-objects-api "
        "for more information on how to register a new object."
    )


def new_class(
    kind: str,
    version: str | None = None,
    asyncio: bool = True,
    namespaced=True,
    scalable: bool | None = None,
    scalable_spec: str | None = None,
    plural: str | None = None,
) -> type[BaseAPIObject]:
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
    if "." in kind:
        kind, version = kind.split(".", 1)
    if version is None:
        version = "v1"
    plural = plural or kind.lower() + "s"
    return type(
        kind,
        (_api_object_class(asyncio),),
        {
            "kind": kind,
            "version": version,
            "endpoint": plural.lower(),
            "plural": plural.lower(),
            "singular": kind.lower(),
            "namespaced": namespaced,
            "scalable": scalable or False,
            "scalable_spec": scalable_spec or "replicas",
        },
    )


def object_from_spec(
    spec: dict,
    api: BaseApi | None = None,
    allow_unknown_type: bool = False,
    _asyncio: bool = True,
) -> BaseAPIObject:
    """Create an APIObject from a Kubernetes resource spec.

    Args:
        spec: A Kubernetes resource spec.
        api: An optional API instance to use.
        allow_unknown_type: Whether to allow unknown resource types.
        _asyncio: Whether to use asyncio or not.

    Returns:
        A corresponding APIObject subclass instance.

    Raises:
        ValueError: If the resource kind or API version is not supported.
    """
    try:
        cls = get_class(spec["kind"], spec["apiVersion"], _asyncio=_asyncio)
    except KeyError:
        if allow_unknown_type:
            cls = new_class(spec["kind"], spec["apiVersion"], asyncio=_asyncio)
        else:
            raise
    return cls(spec, api=api)


def parse_name_type(name: str) -> tuple[str, str, str | None]:
    """Split a ``resource/name`` string into ``(name, kind, version)``."""
    if "/" not in name:
        raise ValueError(f"Invalid name: {name}. Expecting format of 'resource/name'.")
    resource_type, name = name.split("/")
    if "." in resource_type:
        kind = resource_type.split(".")[0]
        version = ".".join(resource_type.split(".")[1:])
    else:
        kind = resource_type
        version = None
    return name, kind, version


def find_object_files(path, recursive: bool = False) -> list:
    """Return the YAML files at a path, which may be a file or a directory."""
    path = pathlib.Path(path)
    if path.is_dir():
        pattern = "**/*.yaml" if recursive else "*.yaml"
        return [f for f in path.glob(pattern) if f.is_file()]
    return [path]


def parse_kind(kind: str) -> tuple[str, str, str]:
    """Parse a Kubernetes resource kind into a tuple of (kind, group, version).

    Args:
        kind: The Kubernetes resource kind.

    Returns:
        A tuple of (kind, group, version).

    Example:
        >>> parse_kind("Pod")
        ("pod", "", "")
        >>> parse_kind("deployment")
        ("deployment", "", "")
        >>> parse_kind("services/v1")
        ("services", "", "v1")
        >>> parse_kind("ingress.networking.k8s.io/v1")
        ("ingress", "networking.k8s.io", "v1")
        >>> parse_kind("role.v1.rbac.authorization.k8s.io")
        ("role", "rbac.authorization.k8s.io", "v1")
    """
    if "/" in kind:
        kind, version = kind.split("/", 1)
    else:
        version = ""
    if "." in kind:
        kind, group = kind.split(".", 1)
    else:
        group = ""
    if "." in group:
        first, potential_group = group.split(".", 1)
        if re.match(r"v\d[a-z0-9]*", first):
            version = first
            group = potential_group
    return kind.lower(), group.lower(), version.lower()
