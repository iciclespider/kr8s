# SPDX-FileCopyrightText: Copyright (c) 2024-2026, Kr8s Developers (See LICENSE for list)
# SPDX-License-Identifier: BSD 3-Clause License
from __future__ import annotations

import pathlib
import threading
import typing
from typing import Any, Generic, Protocol, TypeVar

import anyio
import jsonpath
import yaml

from kr8s._data_utils import dict_list_pack, list_dict_unpack
from kr8s._types import PathType

# TODO Implement set cluster
# TODO Implement delete cluster
# TODO Implement set context
# TODO Implement delete context
# TODO Implement set user
# TODO Implement delete user


class KubeConfigProtocol(Protocol):
    @property
    def raw(self) -> dict: ...


class KubeConfigMixin:

    def get(
        self: KubeConfigProtocol,
        path: str | None = None,
        pointer: str | None = None,
    ) -> Any:
        """Get a value from the config using a JSON Path or JSON Pointer."""
        if not path and not pointer:
            raise ValueError("No path or pointer provided")
        if path:
            return jsonpath.findall(path, self.raw)
        if pointer:
            return jsonpath.pointer.resolve(pointer, self.raw)


class BaseKubeConfig(KubeConfigMixin):
    """A single kubeconfig file or dict.

    Holds everything that doesn't touch the filesystem. Subclasses implement loading and saving.
    """

    def __init__(self, path_or_config: PathType | dict):
        self.path: PathType
        self._raw: dict = {}

        if not path_or_config:
            raise ValueError("KubeConfig path_or_config is None or empty string.")
        if isinstance(path_or_config, str) or isinstance(path_or_config, pathlib.Path):
            self.path = pathlib.Path(path_or_config).expanduser()
            if not self.path.exists():
                raise ValueError(f"File {self.path} does not exist")
            if self.path.is_dir():
                raise IsADirectoryError(
                    f'Error loading config file "{self.path}": is a directory.'
                )
        elif isinstance(path_or_config, dict):
            self._raw = path_or_config
        else:
            raise TypeError("KubeConfig path_or_config must be a string, path or dict.")

    def _save_path(self, path: PathType | None) -> PathType:
        path = self.path if not path else path
        if not path:
            raise ValueError("No path provided")
        return path

    def _dump(self) -> str:
        return yaml.safe_dump(self._raw)

    def _set_namespace(self, namespace: str) -> None:
        for context in self._raw["contexts"]:
            if context["name"] == self.current_context:
                context["context"]["namespace"] = namespace

    def _set_context(self, context: str, allow_unknown: bool = False) -> None:
        if not allow_unknown and context not in [c["name"] for c in self.contexts]:
            raise ValueError(f"Context {context} not found")
        self._raw["current-context"] = context

    def _rename_context(self, old: str, new: str) -> bool:
        """Rename a context and return whether it was the current context."""
        for context in self._raw["contexts"]:
            if context["name"] == old:
                context["name"] = new
                return self.current_context == old
        raise ValueError(f"Context {old} not found")

    def _apply_set(self, pointer: str, value: Any, strict: bool) -> None:
        if strict:
            patch = jsonpath.JSONPatch().replace(pointer, value)
        else:
            patch = jsonpath.JSONPatch().add(pointer, value)
        self._raw = typing.cast(dict, patch.apply(self._raw))

    def _apply_unset(self, pointer: str) -> None:
        patch = jsonpath.JSONPatch().remove(pointer)
        self._raw = typing.cast(dict, patch.apply(self._raw))

    @property
    def current_context(self) -> str:
        return self._raw["current-context"]

    @property
    def current_namespace(self) -> str:
        try:
            current_context = self.current_context
        except KeyError:
            return "default"
        return self.get_context(current_context).get("namespace", "default")

    def get_context(self, context_name: str) -> dict:
        """Get a context by name."""
        for context in self.contexts:
            if context["name"] == context_name:
                return context["context"]
        raise ValueError(f"Context {context_name} not found")

    def get_cluster(self, cluster_name: str) -> dict:
        """Get a cluster by name."""
        for cluster in self.clusters:
            if cluster["name"] == cluster_name:
                return cluster["cluster"]
        raise ValueError(f"Cluster {cluster_name} not found")

    def get_user(self, user_name: str) -> dict:
        """Get a user by name."""
        for user in self.users:
            if user["name"] == user_name:
                return user["user"]
        raise ValueError(f"User {user_name} not found")

    @property
    def raw(self) -> dict:
        return self._raw

    @property
    def preferences(self) -> dict:
        return self._raw.get("preferences", {})

    @property
    def clusters(self) -> list[dict]:
        return self._raw.get("clusters", [])

    @property
    def users(self) -> list[dict]:
        return self._raw.get("users", [])

    @property
    def contexts(self) -> list[dict]:
        return self._raw.get("contexts", [])

    @property
    def extensions(self) -> list[dict]:
        return self._raw.get("extensions", [])


class KubeConfig(BaseKubeConfig):
    """A kubeconfig that loads and saves with blocking I/O.

    The file is read when the object is created.
    """

    def __init__(self, path_or_config: PathType | dict):
        super().__init__(path_or_config)
        self.__write_lock = threading.Lock()
        if not self._raw:
            with open(self.path) as fh:
                self._raw = yaml.safe_load(fh.read())

    def save(self, path=None) -> None:
        path = self._save_path(path)
        with self.__write_lock:
            with open(path, "w") as fh:
                fh.write(self._dump())

    def use_namespace(self, namespace: str) -> None:
        self._set_namespace(namespace)
        self.save()

    def use_context(self, context: str, allow_unknown: bool = False) -> None:
        """Set the current context."""
        self._set_context(context, allow_unknown=allow_unknown)
        self.save()

    def rename_context(self, old: str, new: str) -> None:
        """Rename a context."""
        if self._rename_context(old, new):
            self.use_context(new)
        self.save()

    def set(self, pointer: str, value: Any | None = None, strict: bool = False) -> None:
        """Replace a value using a JSON Pointer."""
        self._apply_set(pointer, value, strict)
        self.save()

    def unset(self, pointer: str) -> Any:
        """Remove a value using a JSON Pointer."""
        self._apply_unset(pointer)
        self.save()


class AsyncKubeConfig(BaseKubeConfig):
    """A kubeconfig that loads and saves with async I/O.

    Await the object to read the file.
    """

    def __init__(self, path_or_config: PathType | dict):
        super().__init__(path_or_config)
        self.__write_lock = anyio.Lock()

    def __await__(self):
        async def f():
            if not self._raw:
                async with await anyio.open_file(self.path) as fh:
                    self._raw = yaml.safe_load(await fh.read())
            return self

        return f().__await__()

    async def save(self, path=None) -> None:
        path = self._save_path(path)
        async with self.__write_lock:
            async with await anyio.open_file(path, "w") as fh:
                await fh.write(self._dump())

    async def use_namespace(self, namespace: str) -> None:
        self._set_namespace(namespace)
        await self.save()

    async def use_context(self, context: str, allow_unknown: bool = False) -> None:
        """Set the current context."""
        self._set_context(context, allow_unknown=allow_unknown)
        await self.save()

    async def rename_context(self, old: str, new: str) -> None:
        """Rename a context."""
        if self._rename_context(old, new):
            await self.use_context(new)
        await self.save()

    async def set(
        self, pointer: str, value: Any | None = None, strict: bool = False
    ) -> None:
        """Replace a value using a JSON Pointer."""
        self._apply_set(pointer, value, strict)
        await self.save()

    async def unset(self, pointer: str) -> Any:
        """Remove a value using a JSON Pointer."""
        self._apply_unset(pointer)
        await self.save()


ConfigT = TypeVar("ConfigT", bound=BaseKubeConfig)


class BaseKubeConfigSet(KubeConfigMixin, Generic[ConfigT]):
    """A set of kubeconfigs merged together, as when KUBECONFIG lists several files."""

    _config_cls: type[ConfigT]

    def __init__(self, *paths_or_dicts: PathType | dict):
        self._configs: list[ConfigT] = []
        for path_or_dict in paths_or_dicts:
            try:
                self._configs.append(self._config_cls(path_or_dict))
            except ValueError:
                pass
        if not self._configs:
            raise ValueError("No valid kubeconfig provided")

    def _check_context(self, context: str) -> None:
        if context not in [c["name"] for c in self.contexts]:
            raise ValueError(f"Context {context} not found")

    def _config_with_context(self, context: str) -> ConfigT:
        for config in self._configs:
            if context in [c["name"] for c in config.contexts]:
                return config
        raise ValueError(f"Context {context} not found")

    @property
    def path(self) -> PathType:
        return self.get_path()

    def get_path(self, context: str | None = None) -> PathType:
        """Return the path of the config for the current context.

        Args:
            context (str): Override the context to use. If not provided, the current context is used.
        """
        if not context:
            context = self.current_context
        if context:
            for config in self._configs:
                if context in [c["name"] for c in config.contexts]:
                    return config.path
        return self._configs[0].path

    @property
    def raw(self) -> dict:
        """Merge all kubeconfig data into a single kubeconfig."""
        data = {
            "apiVersion": "v1",
            "kind": "Config",
            "clusters": self.clusters,
            "users": self.users,
            "contexts": self.contexts,
            "current-context": self.current_context,
        }
        if self.preferences:
            data["preferences"] = self.preferences  # type: ignore[assignment]
        if self.extensions:
            data["extensions"] = self.extensions
        return data

    @property
    def current_context(self) -> str:
        """Return the current context from the first kubeconfig.

        Context configuration from multiples files are ignored.
        """
        return self._configs[0].current_context

    @property
    def current_namespace(self) -> str:
        """Return the current namespace from the current context."""
        try:
            current_context = self.current_context
        except KeyError:
            return "default"
        return self.get_context(current_context).get("namespace", "default")

    def get_context(self, context_name: str) -> dict:
        """Get a context by name."""
        for context in self.contexts:
            if context["name"] == context_name:
                return context["context"]
        raise ValueError(f"Context {context_name} not found")

    def get_cluster(self, cluster_name: str) -> dict:
        """Get a cluster by name."""
        for cluster in self.clusters:
            if cluster["name"] == cluster_name:
                return cluster["cluster"]
        raise ValueError(f"Cluster {cluster_name} not found")

    def get_user(self, user_name: str) -> dict:
        """Get a user by name."""
        for user in self.users:
            if user["name"] == user_name:
                return user["user"]
        raise ValueError(f"User {user_name} not found")

    @property
    def preferences(self) -> dict:
        return self._configs[0].preferences

    @property
    def clusters(self) -> list[dict]:
        clusters = []
        for config in self._configs:
            if config.clusters:
                clusters.extend(config.clusters)
        # Unpack and repack to remove duplicates
        unpacked = list_dict_unpack(clusters, "name", "cluster")
        repacked = dict_list_pack(unpacked, "name", "cluster")
        return repacked

    @property
    def users(self) -> list[dict]:
        users = []
        for config in self._configs:
            if config.users:
                users.extend(config.users)
        # Unpack and repack to remove duplicates
        unpacked = list_dict_unpack(users, "name", "user")
        repacked = dict_list_pack(unpacked, "name", "user")
        return repacked

    @property
    def contexts(self) -> list[dict]:
        contexts = []
        for config in self._configs:
            if config.contexts:
                contexts.extend(config.contexts)
        # Unpack and repack to remove duplicates
        unpacked = list_dict_unpack(contexts, "name", "context")
        repacked = dict_list_pack(unpacked, "name", "context")
        return repacked

    @property
    def extensions(self) -> list[dict]:
        extensions = []
        for config in self._configs:
            if config.extensions:
                extensions.extend(config.extensions)
        return extensions


class KubeConfigSet(BaseKubeConfigSet[KubeConfig]):
    _config_cls = KubeConfig

    def save(self):
        for config in self._configs:
            config.save()

    def use_namespace(self, namespace: str) -> None:
        current_context = self.current_context
        for config in self._configs:
            for context in config._raw["contexts"]:
                if context["name"] == current_context:
                    context["context"]["namespace"] = namespace
            config.save()

    def use_context(self, context: str) -> None:
        """Set the current context."""
        self._check_context(context)
        self._configs[0].use_context(context, allow_unknown=True)

    def rename_context(self, old: str, new: str) -> None:
        """Rename a context."""
        was_current = self.current_context == old
        self._config_with_context(old).rename_context(old, new)
        if was_current:
            self.use_context(new)

    def set(self, pointer: str, value) -> None:
        """Replace a value using a JSON Pointer.

        Set only applies to the first kubeconfig.
        """
        self._configs[0].set(pointer=pointer, value=value)

    def unset(self, pointer: str) -> None:
        """Remove a value using a JSON Pointer.

        Unset applies to all kubeconfigs.
        """
        for config in self._configs:
            try:
                config.unset(pointer=pointer)
            except Exception:
                pass


class AsyncKubeConfigSet(BaseKubeConfigSet[AsyncKubeConfig]):
    _config_cls = AsyncKubeConfig

    def __await__(self):
        async def f():
            for config in self._configs:
                await config
            return self

        return f().__await__()

    async def save(self):
        for config in self._configs:
            await config.save()

    async def use_namespace(self, namespace: str) -> None:
        current_context = self.current_context
        for config in self._configs:
            for context in config._raw["contexts"]:
                if context["name"] == current_context:
                    context["context"]["namespace"] = namespace
            await config.save()

    async def use_context(self, context: str) -> None:
        """Set the current context."""
        self._check_context(context)
        await self._configs[0].use_context(context, allow_unknown=True)

    async def rename_context(self, old: str, new: str) -> None:
        """Rename a context."""
        was_current = self.current_context == old
        await self._config_with_context(old).rename_context(old, new)
        if was_current:
            await self.use_context(new)

    async def set(self, pointer: str, value) -> None:
        """Replace a value using a JSON Pointer.

        Set only applies to the first kubeconfig.
        """
        await self._configs[0].set(pointer=pointer, value=value)
        await anyio.sleep(0)

    async def unset(self, pointer: str) -> None:
        """Remove a value using a JSON Pointer.

        Unset applies to all kubeconfigs.
        """
        for config in self._configs:
            try:
                await config.unset(pointer=pointer)
            except Exception:
                pass
