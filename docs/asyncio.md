# Asyncio

`kr8s` provides both a synchronous and an asynchronous API. The synchronous API is the default.

```python
import kr8s

pods = kr8s.get("pods")
```

Like [httpx](https://www.python-httpx.org/), each API does its own I/O. The sync API makes blocking calls with an `httpx2.Client`, and the async API awaits an `httpx2.AsyncClient`. Both are built on shared base classes, so they behave the same and have the same objects, method signatures and return values.

```{note}
The sync API blocks the thread it is called from. You can still call it from inside an async context, which is common in [IPython](https://ipython.org/) and [Jupyter](https://jupyter.org/) because they run your code inside an event loop. While a sync call is waiting on the Kubernetes API, though, that event loop can't run other tasks. If you're writing async code, use `kr8s.asyncio`.
```

## `kr8s.asyncio` submodule

For users that want to use `kr8s` with `asyncio` or `trio` you can find the async API is directly available via `kr8s.asyncio`.

```python
import kr8s.asyncio

pods = [pod async for pod in kr8s.asyncio.get("pods")]
```

Submodules including `kr8s.objects` and `kr8s.portforward` also have `asyncio` equivalents at `kr8s.asyncio.objects` and `kr8s.asyncio.portforward`.

The sync and async classes are siblings that share a base class, so a sync object is not an instance of its async equivalent. For example `isinstance(kr8s.objects.Pod(...), kr8s.asyncio.objects.Pod)` is `False`. To check for any `Pod` regardless of API, use the shared base class `kr8s._objects.BasePod`.

## Documentation examples

Throughout the `kr8s` documentation we strive to provide code examples using both the sync and async APIs, you will see tab selectors like this which allows you to easily compare the two APIs and choose the one you wish to use.

`````{tab-set}

````{tab-item} Sync
:sync: sync
```python
from kr8s.objects import Pod

pod = Pod({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "my-pod",
        },
        "spec": {
            "containers": [{"name": "pause", "image": "gcr.io/google_containers/pause",}]
        },
    })

pod.create()
```
````

````{tab-item} Async
:sync: async
```python
from kr8s.asyncio.objects import Pod

pod = await Pod({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {
            "name": "my-pod",
        },
        "spec": {
            "containers": [{"name": "pause", "image": "gcr.io/google_containers/pause",}]
        },
    })

await pod.create()
```
````

`````
