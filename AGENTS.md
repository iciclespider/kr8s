# AGENTS.md

Guidance for AI agents (and humans) working in the `kr8s` repository.

`kr8s` is a simple, extensible Python client library for Kubernetes that feels
familiar to people who already know `kubectl`. It ships a synchronous API by
default and an async API (`asyncio`/`trio`) under `kr8s.asyncio`.

## Repository layout

- `kr8s/` — the library source.
  - `_objects.py` — `BaseAPIObject` and a `Base<Kind>` class for every
    Kubernetes resource (`BasePod`, `BaseDeployment`, …) holding attributes and
    all I/O-free logic, plus `get_class`, `new_class` and `parse_kind`.
  - `objects.py` — the **sync** public objects: `APIObject` and every resource
    class, doing blocking I/O.
  - `asyncio/objects.py` — the **async** public objects, doing async I/O.
  - `_api.py` — `BaseApi`, the sync `Api` (`httpx2.Client`) and `AsyncApi`
    (`httpx2.AsyncClient`): auth, request dispatch, caching.
  - `_auth.py`, `_config.py` — kubeconfig loading, auth, and cluster config, as
    `BaseKubeAuth` / `KubeAuth` / `AsyncKubeAuth` and `BaseKubeConfig` /
    `KubeConfig` / `AsyncKubeConfig` (plus the `*KubeConfigSet` variants).
  - `_exec.py`, `_portforward.py` — `BaseExec` / `Exec` / `AsyncExec` and
    `BasePortForward`. The port forwards themselves are in `portforward.py`
    (sync, threads) and `asyncio/portforward.py` (asyncio).
  - `__init__.py` and `asyncio/` — the top-level sync and async helpers
    (`kr8s.get`, `kr8s.asyncio.get`, …).
  - `_async_utils.py` — small helpers for the async implementations.
  - `_data_utils.py`, `_exceptions.py` — helpers.
  - `tests/` — pytest suite (`test_*.py`), plus `resources/` and `scripts/`.
  - `conftest.py` (both root and `kr8s/`) — shared fixtures.
- `docs/` — Sphinx documentation (MyST markdown). `contributing.md` and
  `releasing.md` are the canonical process docs.
- `examples/kubectl-ng/` — a separate `kubectl-ng` package (a `kubectl` clone
  built on kr8s) with its **own** `pyproject.toml` and test suite.
- `ci/` — automation scripts (e.g. `update-kubernetes.py`).
- `.github/workflows/` — CI (`test-kr8s.yaml`, `test-kubectl-ng.yaml`, release).

## The sync/async architecture (read this before editing resource code)

kr8s follows the same pattern as httpx (`BaseClient` → `Client` /
`AsyncClient`). Every type that does I/O has three classes:

- `BaseX` holds state, properties and every decision that doesn't need I/O:
  building URLs, params and payloads, parsing responses, checking conditions.
  It never awaits or blocks.
- `X` (sync) and `AsyncX` (async) subclass the base and only do the I/O. Each
  method should read as "build with a base helper → do the request → parse with
  a base helper".

The sync side does real blocking I/O (`httpx2.Client`, sync websockets,
threads). It never runs async code, so there's no background event loop.

When you add or change behaviour:

1. Put any logic that doesn't need I/O on the `Base*` class, usually as a
   private helper.
2. Implement the I/O in **both** the sync class (`kr8s/objects.py`, `_api.py`'s
   `Api`, …) and the async class (`kr8s/asyncio/objects.py`, `AsyncApi`, …),
   keeping their signatures the same apart from `async`.
3. Use the same method names on both sides. Don't add `async_`-prefixed
   twins of public methods.

Resource classes combine their base with the flavour's `APIObject`, base first:
`class Pod(BasePod, APIObject)`. `get_class` and `new_class` choose sync or
async classes using the `_asyncio` class attribute.

## Development environment

Use [`uv`](https://github.com/astral-sh/uv) (the project's chosen tool) but
anything that installs the package in editable mode works.

```bash
pip install uv
uv sync --dev
```

Work on branches in git worktrees rather than switching branches in place.
Create worktrees under a `.worktrees/` directory in the project root
(e.g. `git worktree add .worktrees/my-feature -b my-feature`). This directory is
gitignored so worktrees are never tracked in version control.

Common tasks are defined via [`taskipy`](https://github.com/taskipy/taskipy) in
`pyproject.toml`:

- `uv run task test` — run the test suite.
- `uv run task test-ci` — tests with retries (what CI runs).
- `uv run task docs` — build docs once.
- `uv run task docs-serve` — live-reloading docs server.

## Testing

Tests run with `pytest` and use [`kind`](https://kind.sigs.k8s.io/) via
[`pytest-kind`](https://pypi.org/project/pytest-kind/) to spin up a real
Kubernetes cluster in Docker. **Docker must be available.** Cluster lifecycle is
handled by fixtures, so no manual cluster setup is needed.

```bash
uv run task test
# or a single file / test
uv run pytest kr8s/tests/test_objects.py
uv run pytest kr8s/tests/test_objects.py::test_pod_create
```

Key testing facts:

- Tests are **integration-heavy** and hit a live kind cluster; they are slower
  than pure unit tests. A per-test timeout of 300s is configured.
- `pytest-asyncio` runs in `asyncio_mode = "auto"`, so `async def test_*`
  functions work without decorators.
- Each test gets a fresh namespace via the autouse `ns` fixture
  (`kr8s/conftest.py`); use it to avoid cross-test collisions.
- Useful fixtures: `example_pod_spec`, `example_deployment_spec`,
  `example_service_spec`, `example_crd_spec`, `serviceaccount`, `k8s_cluster`.
- Prefer standard-library `unittest.mock` (`Mock`, `AsyncMock`, `patch`, and
  `patch.object`) for test doubles and temporary replacements. Avoid pytest's
  `monkeypatch` when `unittest.mock` can express the same behavior.
- Set `KUBERNETES_VERSION` to test against a specific k8s version. CI runs a
  matrix of Python 3.12–3.15 and multiple Kubernetes versions.
- Test/`conftest.py`/`examples`/`docs`/`ci` code is exempt from docstring (`D`),
  naming (`N`), and bugbear (`B`) lint rules.

## Linting, formatting, and types

Managed by [`pre-commit`](https://pre-commit.com/). Install and run it:

```bash
pip install pre-commit
pre-commit install            # run automatically on each commit
pre-commit run --all-files    # run manually against everything
```

The hooks (see `.pre-commit-config.yaml`) are:

- **black** — formatting.
- **ruff** — linting with autofix (`select = ["D","E","W","F","I","N","B"]`,
  Google docstring convention, line length 120). Public functions/classes need
  Google-style docstrings.
- **insert-license** — every `.py` file must carry the SPDX header from
  `LICENSE_HEADER` (auto-inserted). Keep it at the top of new files.
- **mypy** — type checking (excludes `examples`, `ci`, `docs`).
  The library is typed and ships `py.typed`; keep annotations correct.
- **pyupgrade** — enforces modern syntax (`--py312-plus`); the project targets
  Python **3.12.1+**, so don't use syntax newer than 3.12 in library code.

## Conventions

- **Python 3.12.1+ compatibility** in `kr8s/` library code. Use
  `from __future__ import annotations` for modern typing syntax.
- Keep code **human-readable** — a project goal is no swagger/generated code.
- Public API changes should be reflected in `docs/` (MyST markdown). Where docs
  show code, provide **both** sync and async examples using the `tab-set`
  directive (see `docs/asyncio.md`).
- New resource kinds go in `kr8s/_objects.py` and must be exported from both
  `kr8s/objects.py` (sync) and `kr8s/asyncio/objects.py` (async) `__all__`.

## Issues

When opening a GitHub issue, **always fill in the appropriate template.** Read
the template file and complete every required field rather than posting a
free-form body.

Keep issues **concise** — include only the information needed to reproduce the
problem (for bugs) or to explain the value of the change (for feature requests).
Link and cross-reference any supporting material (logs, related issues/PRs,
docs) rather than pasting it inline. Brevity is key.

Issue templates are GitHub form templates (YAML) — map their fields into the
body you submit (e.g. via `gh issue create`), including the required `project`
choice (`kr8s` or `kubectl-ng`):

- Bug reports — `.github/ISSUE_TEMPLATE/bug-report.yml`
  (required: which project, "What happened?"; optional: "Anything else?").
- Feature requests — `.github/ISSUE_TEMPLATE/feature-request.yml`
  (required: which project, "What do you need?").
- General coding questions are directed to Stack Overflow (`#kr8s` tag), not the
  issue tracker — see `.github/ISSUE_TEMPLATE/config.yml`.

## Versioning & releasing

- Versioning follows [EffVer](https://jacobtomlinson.dev/effver); tags are
  prefixed `v` (e.g. `v0.20.15`). Version is derived from git tags via
  `hatch-vcs` — do not hand-edit `kr8s/_version.py`.
- Releasing (maintainers): create a `v0.0.0` tag and push it to the
  `kr8s-org/kr8s` upstream; the release GitHub Action builds and publishes to
  PyPI. See `docs/releasing.md`.

## Working with kubectl-ng

`examples/kubectl-ng/` is an independent package with its own `pyproject.toml`,
`uv.lock`, and tests, and it pins a specific released `kr8s` version. Develop it
from inside that directory (`uv sync --dev` there). It is covered by the
separate `test-kubectl-ng.yaml` workflow, not the main test workflow.

## Quick checklist before finishing a change

1. Put I/O-free logic on the `Base*` class.
2. Implemented the I/O in **both** the sync and async classes, and added new
   kinds as `Base<Kind>` in `_objects.py` plus a class in each `objects.py`
   module (exported from `kr8s/asyncio/objects.py` `__all__`).
3. Added/updated tests in `kr8s/tests/` (they will spin up kind).
4. `uv run task test` (or the relevant subset) passes.
5. `pre-commit run --all-files` is clean (black, ruff, mypy, license header).
6. Updated `docs/` with sync + async examples where user-facing behavior changed.
