# SPDX-FileCopyrightText: Copyright (c) 2023-2026, Kr8s Developers (See LICENSE for list)
# SPDX-License-Identifier: BSD 3-Clause License
"""Helpers for the async implementations."""

from __future__ import annotations

import builtins
import subprocess
import tempfile
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import anyio
import anyio.to_thread


async def check_output(*args, **kwargs) -> str:
    """Run a command and return its output."""
    completed_process = await anyio.run_process(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        **kwargs,
    )
    if completed_process.returncode == 0:
        return completed_process.stdout.decode()
    else:
        raise RuntimeError(
            f"Process exited with non-zero code {completed_process.returncode}:\n{completed_process.stderr.decode()}"
        )


@asynccontextmanager
async def NamedTemporaryFile(  # noqa: N802
    *args, delete: bool = True, **kwargs
) -> AsyncGenerator[anyio.Path]:
    """Create a temporary file that is deleted when the context exits."""
    kwargs.update(delete=False)

    def f():
        return tempfile.NamedTemporaryFile(*args, **kwargs)

    tmp = await anyio.to_thread.run_sync(f)
    fh = anyio.Path(tmp.name)
    yield fh
    if delete:
        await fh.unlink()


anext = builtins.anext
