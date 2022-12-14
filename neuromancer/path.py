from __future__ import annotations

import contextlib
import os
import pathlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from _typeshed import StrPath


@contextlib.contextmanager
def run_in_dir(dir: StrPath):
    dir = pathlib.Path(dir)
    dir.mkdir(exist_ok=True, parents=True)
    cwd = pathlib.Path.cwd()

    try:
        os.chdir(dir)
        yield
    finally:
        os.chdir(cwd)
