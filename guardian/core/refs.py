"""Resolve ``"module:attr"`` references used in pipeline specs."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from typing import Any


def load_ref(ref: str, registry: Mapping[str, Any] | None = None) -> Any:
    """Return the object named by ``ref``.

    ``registry`` is consulted first (exact key match), which lets callers and tests
    inject objects without importable modules. Otherwise ``ref`` must look like
    ``"package.module:attr.sub"``. A module that is not importable as written is
    retried under the ``guardian.`` package, so ``demo.blocks:ingest`` resolves to
    ``guardian.demo.blocks``.
    """
    if registry is not None and ref in registry:
        return registry[ref]
    module_name, sep, attr_path = ref.partition(":")
    if not sep or not module_name or not attr_path:
        raise ValueError(f"invalid reference {ref!r}: expected 'module:attr'")
    try:
        obj: Any = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name != module_name.split(".")[0] and exc.name != module_name:
            raise  # the module exists but one of its own imports is missing
        obj = importlib.import_module(f"guardian.{module_name}")
    for part in attr_path.split("."):
        obj = getattr(obj, part)
    return obj
