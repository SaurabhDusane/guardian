"""Load a pipeline spec from YAML and validate it as a DAG."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from guardian.core.models import BlockSpec, FallbackEdge, PipelineSpec, ShadowPolicy

_PIPELINE_KEYS = {"name", "blocks"}
_BLOCK_KEYS = {
    "name",
    "fn",
    "inputs",
    "schema",
    "quarantine_threshold",
    "fallbacks",
    "params",
    "merge_key",
    "versions",
    "active",
    "load",
    "shadow",
}
_SHADOW_KEYS = {"required_runs", "max_changed_fraction", "min_pass_rate"}
_FALLBACK_KEYS = {"replaces", "source", "adapter"}


class SpecError(ValueError):
    """The pipeline spec is malformed or does not describe a valid DAG."""


def load_spec(path: Path | str) -> PipelineSpec:
    path = Path(path)
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise SpecError(f"{path}: invalid YAML: {exc}") from exc
    return parse_spec(raw, source=str(path))


def parse_spec(raw: Any, source: str = "<spec>") -> PipelineSpec:
    if not isinstance(raw, Mapping):
        raise SpecError(f"{source}: top level must be a mapping with 'name' and 'blocks'")
    _check_keys(raw, _PIPELINE_KEYS, source)
    name = raw.get("name")
    if not isinstance(name, str) or not name:
        raise SpecError(f"{source}: 'name' must be a non-empty string")
    blocks_raw = raw.get("blocks")
    if not isinstance(blocks_raw, list) or not blocks_raw:
        raise SpecError(f"{source}: 'blocks' must be a non-empty list")

    blocks = []
    for i, block_raw in enumerate(blocks_raw):
        where = f"{source}: blocks[{i}]"
        if not isinstance(block_raw, Mapping):
            raise SpecError(f"{where}: must be a mapping")
        if isinstance(block_raw.get("name"), str):
            where = f"{source}: block {block_raw['name']!r}"
        blocks.append(_parse_block(block_raw, where))

    try:
        spec = PipelineSpec(name=name, blocks=tuple(blocks))
    except ValueError as exc:
        raise SpecError(f"{source}: {exc}") from exc
    validate_dag(spec, source)
    return spec


def _parse_block(raw: Mapping[str, Any], where: str) -> BlockSpec:
    _check_keys(raw, _BLOCK_KEYS, where)
    if not isinstance(raw.get("name"), str) or not raw["name"]:
        raise SpecError(f"{where}: 'name' is required and must be a string")
    versions = raw.get("versions") or {}
    if not isinstance(versions, Mapping) or not all(
        isinstance(k, str) and isinstance(v, str) and v for k, v in versions.items()
    ):
        raise SpecError(f"{where}: 'versions' must map version names to 'module:fn' strings")
    active = raw.get("active")
    if versions:
        if active is None:
            raise SpecError(f"{where}: 'versions' requires 'active'")
        if active not in versions:
            raise SpecError(f"{where}: active version {active!r} is not one of {sorted(versions)}")
        if raw.get("fn") is not None and raw["fn"] != versions[active]:
            raise SpecError(
                f"{where}: 'fn' conflicts with the active version; omit 'fn' when "
                "declaring 'versions'"
            )
        fn = versions[active]
    else:
        if active is not None:
            raise SpecError(f"{where}: 'active' requires 'versions'")
        fn = raw.get("fn")
        if not isinstance(fn, str) or not fn:
            raise SpecError(f"{where}: 'fn' is required and must be a string")
    load = raw.get("load")
    if load is not None and not isinstance(load, str):
        raise SpecError(f"{where}: 'load' must be a 'module:attr' string")
    shadow_raw = raw.get("shadow") or {}
    if not isinstance(shadow_raw, Mapping):
        raise SpecError(f"{where}: 'shadow' must be a mapping")
    _check_keys(shadow_raw, _SHADOW_KEYS, f"{where} shadow")
    inputs = raw.get("inputs") or []
    if not isinstance(inputs, list) or not all(isinstance(x, str) for x in inputs):
        raise SpecError(f"{where}: 'inputs' must be a list of block names")
    schema = raw.get("schema")
    if schema is not None and not isinstance(schema, str):
        raise SpecError(f"{where}: 'schema' must be a 'module:attr' string")
    threshold = raw.get("quarantine_threshold", 0.0)
    if isinstance(threshold, bool) or not isinstance(threshold, int | float):
        raise SpecError(f"{where}: 'quarantine_threshold' must be a number")
    params = raw.get("params") or {}
    if not isinstance(params, Mapping):
        raise SpecError(f"{where}: 'params' must be a mapping")
    merge_key = raw.get("merge_key")
    if merge_key is not None and (
        not isinstance(merge_key, list) or not all(isinstance(c, str) for c in merge_key)
    ):
        raise SpecError(f"{where}: 'merge_key' must be a list of column names")
    fallbacks_raw = raw.get("fallbacks") or []
    if not isinstance(fallbacks_raw, list):
        raise SpecError(f"{where}: 'fallbacks' must be a list")

    try:
        fallbacks = tuple(
            _parse_fallback(f, f"{where} fallbacks[{j}]") for j, f in enumerate(fallbacks_raw)
        )
        return BlockSpec(
            name=raw["name"],
            fn=fn,
            inputs=tuple(inputs),
            schema=schema,
            quarantine_threshold=float(threshold),
            fallbacks=fallbacks,
            params=dict(params),
            merge_key=tuple(merge_key) if merge_key is not None else None,
            versions=dict(versions),
            active=active,
            load=load,
            shadow=ShadowPolicy(**shadow_raw),
        )
    except ValueError as exc:
        if isinstance(exc, SpecError):
            raise
        raise SpecError(f"{where}: {exc}") from exc


def _parse_fallback(raw: Any, where: str) -> FallbackEdge:
    if not isinstance(raw, Mapping):
        raise SpecError(f"{where}: must be a mapping")
    _check_keys(raw, _FALLBACK_KEYS, where)
    for key in ("replaces", "source"):
        if not isinstance(raw.get(key), str):
            raise SpecError(f"{where}: '{key}' is required and must be a string")
    adapter = raw.get("adapter")
    if adapter is not None and not isinstance(adapter, str):
        raise SpecError(f"{where}: 'adapter' must be a 'module:attr' string")
    return FallbackEdge(replaces=raw["replaces"], source=raw["source"], adapter=adapter)


def _check_keys(raw: Mapping[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(raw) - allowed)
    if unknown:
        raise SpecError(f"{where}: unknown key(s) {unknown}; allowed: {sorted(allowed)}")


def dependencies(spec: PipelineSpec) -> dict[str, list[str]]:
    """Blocks each block must run after: its inputs plus its fallback sources."""
    deps: dict[str, list[str]] = {}
    for block in spec.blocks:
        extra = [e.source for e in block.fallbacks if e.source not in block.inputs]
        deps[block.name] = [*block.inputs, *extra]
    return deps


def validate_dag(spec: PipelineSpec, source: str = "<spec>") -> None:
    names = set(spec.block_names)
    for block in spec.blocks:
        for upstream in block.inputs:
            if upstream not in names:
                raise SpecError(f"{source}: block {block.name!r} has unknown input {upstream!r}")
            if upstream == block.name:
                raise SpecError(f"{source}: block {block.name!r} lists itself as an input")
        for edge in block.fallbacks:
            if edge.source not in names:
                raise SpecError(
                    f"{source}: block {block.name!r} has a fallback to nonexistent block "
                    f"{edge.source!r}"
                )
            if edge.source == block.name:
                raise SpecError(f"{source}: block {block.name!r} cannot fall back to itself")
    topological_order(spec, source)


def topological_order(spec: PipelineSpec, source: str = "<spec>") -> list[str]:
    """Deterministic topological order (spec order among ready blocks).

    Raises SpecError naming the cycle if there is one.
    """
    deps = dependencies(spec)
    order: list[str] = []
    done: set[str] = set()
    remaining = list(spec.block_names)
    while remaining:
        ready = [b for b in remaining if all(d in done for d in deps[b])]
        if not ready:
            raise SpecError(
                f"{source}: dependency cycle: {' -> '.join(_find_cycle(deps, remaining))}"
            )
        for b in ready:
            order.append(b)
            done.add(b)
        remaining = [b for b in remaining if b not in done]
    return order


def _find_cycle(deps: dict[str, list[str]], nodes: list[str]) -> list[str]:
    pending = set(nodes)
    for start in nodes:
        path: list[str] = []
        seen: dict[str, int] = {}
        node = start
        while node not in seen:
            seen[node] = len(path)
            path.append(node)
            node = next(d for d in deps[node] if d in pending)
        return [*path[seen[node] :], node]
    return nodes
