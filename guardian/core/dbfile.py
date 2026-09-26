"""Connections to a DuckDB file: per operation by default, shared within a session.

Guardian's stores open the file for each operation and close it right after. An idle
Guardian therefore never holds the file, and another process (the CLI next to a
Dagster deployment, say) can always open it. Opening costs time, though: a pipeline run
performs dozens of operations per store. Inside ``session()`` a store keeps a single
connection open instead, for one unit of work (a pipeline run, a block, a promotion),
and releases the file when the session ends. Sessions nest.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb


class DuckDBFile:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self._con: duckdb.DuckDBPyConnection | None = None
        self._depth = 0
        self._lock = threading.RLock()

    @contextmanager
    def connect(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """A connection for one operation: inside a session, the session's (opened on
        first use); otherwise a fresh one, closed right after."""
        with self._lock:
            if self._depth:
                if self._con is None:
                    self._con = duckdb.connect(str(self.path))
                yield self._con
                return
        con = duckdb.connect(str(self.path))
        try:
            yield con
        finally:
            con.close()

    @contextmanager
    def session(self) -> Iterator[None]:
        """Share one connection, opened lazily, until the (outermost) session ends."""
        with self._lock:
            self._depth += 1
        try:
            yield
        finally:
            with self._lock:
                self._depth -= 1
                if self._depth == 0 and self._con is not None:
                    self._con.close()
                    self._con = None
