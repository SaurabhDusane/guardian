"""Immutable block-output snapshots plus a per-block last-good pointer."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

import pandas as pd

from guardian.core.models import DataRef, GuardianError, validate_name


class SnapshotError(GuardianError):
    pass


class SnapshotExistsError(SnapshotError):
    """Raised on an attempt to overwrite an existing (immutable) snapshot."""


class SnapshotNotFoundError(SnapshotError, KeyError):
    pass


@runtime_checkable
class SnapshotStore(Protocol):
    def write(self, block: str, run_id: str, df: pd.DataFrame) -> DataRef:
        """Persist ``df`` as the immutable snapshot (block, run_id)."""
        ...

    def read(self, block: str, run_id: str) -> pd.DataFrame: ...

    def exists(self, block: str, run_id: str) -> bool: ...

    def list_runs(self, block: str) -> list[str]:
        """Run ids with a snapshot for ``block``, oldest first."""
        ...

    def mark_last_good(self, block: str, run_id: str) -> DataRef: ...

    def last_good(self, block: str) -> DataRef | None: ...

    def write_provenance(self, block: str, run_id: str, provenance: Mapping[str, Any]) -> None:
        """Record where (block, run_id)'s inputs came from. Immutable, like snapshots."""
        ...

    def read_provenance(self, block: str, run_id: str) -> dict[str, Any] | None: ...


def _atomic_write_bytes(path: Path, data: bytes) -> None:
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


class LocalParquetSnapshotStore:
    """Snapshots stored as ``<root>/snapshots/<block>/<run_id>.parquet``.

    The last-good pointer for each block lives in ``<block>/_last_good.json``.
    """

    LAST_GOOD_FILE = "_last_good.json"

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root) / "snapshots"
        self.root.mkdir(parents=True, exist_ok=True)

    def _block_dir(self, block: str) -> Path:
        return self.root / validate_name(block, "block name")

    def _path(self, block: str, run_id: str) -> Path:
        return self._block_dir(block) / f"{validate_name(run_id, 'run_id')}.parquet"

    def write(self, block: str, run_id: str, df: pd.DataFrame) -> DataRef:
        path = self._path(block, run_id)
        if path.exists():
            raise SnapshotExistsError(f"snapshot ({block}, {run_id}) already exists")
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
        try:
            df.to_parquet(tmp, engine="pyarrow")
            if path.exists():  # lost a race with another writer
                raise SnapshotExistsError(f"snapshot ({block}, {run_id}) already exists")
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)
        return DataRef(block=block, run_id=run_id)

    def read(self, block: str, run_id: str) -> pd.DataFrame:
        path = self._path(block, run_id)
        if not path.exists():
            raise SnapshotNotFoundError(f"no snapshot ({block}, {run_id})")
        return pd.read_parquet(path, engine="pyarrow")

    def exists(self, block: str, run_id: str) -> bool:
        return self._path(block, run_id).exists()

    def list_runs(self, block: str) -> list[str]:
        block_dir = self._block_dir(block)
        if not block_dir.exists():
            return []
        files = [p for p in block_dir.glob("*.parquet") if not p.name.startswith(".")]
        files.sort(key=lambda p: (p.stat().st_mtime_ns, p.stem))
        return [p.stem for p in files]

    def mark_last_good(self, block: str, run_id: str) -> DataRef:
        if not self.exists(block, run_id):
            raise SnapshotNotFoundError(f"cannot mark missing snapshot ({block}, {run_id})")
        pointer = {"run_id": run_id, "promoted_at": datetime.now(UTC).isoformat()}
        _atomic_write_bytes(
            self._block_dir(block) / self.LAST_GOOD_FILE,
            json.dumps(pointer).encode("utf-8"),
        )
        return DataRef(block=block, run_id=run_id)

    def last_good(self, block: str) -> DataRef | None:
        pointer = self._block_dir(block) / self.LAST_GOOD_FILE
        if not pointer.exists():
            return None
        run_id = json.loads(pointer.read_text(encoding="utf-8"))["run_id"]
        return DataRef(block=block, run_id=run_id)

    def _provenance_path(self, block: str, run_id: str) -> Path:
        return self._block_dir(block) / f"{validate_name(run_id, 'run_id')}.provenance.json"

    def write_provenance(self, block: str, run_id: str, provenance: Mapping[str, Any]) -> None:
        path = self._provenance_path(block, run_id)
        if path.exists():
            raise SnapshotExistsError(f"provenance for ({block}, {run_id}) already exists")
        path.parent.mkdir(parents=True, exist_ok=True)
        _atomic_write_bytes(path, json.dumps(provenance, sort_keys=True, default=str).encode())

    def read_provenance(self, block: str, run_id: str) -> dict[str, Any] | None:
        path = self._provenance_path(block, run_id)
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
