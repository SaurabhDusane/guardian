"""Code fingerprints: which implementation actually produced a block's output.

Every block run records a fingerprint (sha256 of the function's source) of the code
that ran, and the source itself is kept once per fingerprint under ``<root>/code/``.
Comparing the fingerprint of a failing run with that of the last good run shows
whether the block's code changed in between, and the stored sources give the diff.
"""

from __future__ import annotations

import contextlib
import functools
import hashlib
import inspect
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from guardian.core.snapshots import _atomic_write_bytes

CODE_DIR = "code"


@dataclass(frozen=True)
class CodeFingerprint:
    sha: str  # sha256 of ``source`` (or of ``qualname`` when the source is unavailable)
    ref: str  # the reference the spec resolved ("module:fn" or a registry key)
    qualname: str  # "module:qualname" of the function that actually ran
    file: str | None  # its source file, when known
    source: str | None  # its source text, when available


def _implementation(fn: Any) -> Any:
    """The function doing the work: partials unwrapped, then ``__wrapped__`` followed."""
    # A functools.wraps wrapper (e.g. output fault injection) is not a code change; a
    # replacement written without wraps is.
    while isinstance(fn, functools.partial):
        fn = fn.func
    with contextlib.suppress(ValueError):  # a __wrapped__ cycle
        fn = inspect.unwrap(fn)
    while isinstance(fn, functools.partial):
        fn = fn.func
    return fn


def fingerprint(fn: Any, ref: str) -> CodeFingerprint:
    impl = _implementation(fn)
    module = getattr(impl, "__module__", None) or "?"
    qualname = f"{module}:{getattr(impl, '__qualname__', type(impl).__qualname__)}"
    try:
        source: str | None = inspect.getsource(impl)
    except (OSError, TypeError):
        source = None
    try:
        file = inspect.getsourcefile(impl)
    except TypeError:
        file = None
    digest = hashlib.sha256((source if source is not None else qualname).encode("utf-8"))
    return CodeFingerprint(digest.hexdigest()[:16], ref, qualname, file, source)


class CodeStore:
    """Content-addressed store of the sources that ran: ``<root>/code/<sha>.json``."""

    def __init__(self, root: Path | str) -> None:
        self.dir = Path(root) / CODE_DIR

    def put(self, fp: CodeFingerprint) -> None:
        path = self.dir / f"{fp.sha}.json"
        if path.exists():
            return
        self.dir.mkdir(parents=True, exist_ok=True)
        _atomic_write_bytes(path, json.dumps(asdict(fp), sort_keys=True).encode("utf-8"))

    def get(self, sha: str) -> CodeFingerprint | None:
        path = self.dir / f"{sha}.json"
        if not sha or not path.exists():
            return None
        return CodeFingerprint(**json.loads(path.read_text(encoding="utf-8")))
