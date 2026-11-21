"""SQLite archive for complete traces plus the sampler's durable ingest state.

A trace is written in a single transaction: the trace row, every span row and
the archive bookkeeping either all land or none do.  Only after ``commit()``
returns does the sampler release the trace's slot.

The same database also carries the collector's write-ahead state so a process
that was killed can be restarted on the same volume without losing anything
that was acknowledged:

* ``windows`` — one row per trace id: windows still collecting, traces waiting
  to be archived and recently closed ids (so late spans stay rejected after a
  restart).
* ``staged_spans`` — every span acknowledged by ``/v1/traces`` whose window is
  still collecting or waiting to be written.  Rows leave the table when the
  window is discarded or when its archive commit lands.

The archive commit, the removal of the trace's staged spans and the window's
closed marker happen in one transaction: a crash before the commit leaves a
pending window that recovery writes again; a crash after it leaves a closed
window that is never archived twice.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Iterable, Optional

from .models import ActiveTrace, SpanRecord

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS archived_traces (
    trace_id        TEXT PRIMARY KEY,
    first_seen_ns   INTEGER NOT NULL,
    last_span_ns    INTEGER NOT NULL,
    archived_at_ns  INTEGER NOT NULL,
    span_count      INTEGER NOT NULL,
    error_count     INTEGER NOT NULL,
    root_name       TEXT,
    root_span_id    TEXT,
    service_names   TEXT NOT NULL DEFAULT '[]',
    min_start_ns    INTEGER,
    max_end_ns      INTEGER,
    duration_ns     INTEGER
);

CREATE TABLE IF NOT EXISTS archived_spans (
    trace_id        TEXT NOT NULL,
    span_id         TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    parent_span_id  TEXT,
    name            TEXT NOT NULL DEFAULT '',
    kind            TEXT NOT NULL DEFAULT 'UNSPECIFIED',
    status_code     TEXT NOT NULL DEFAULT 'UNSET',
    status_message  TEXT NOT NULL DEFAULT '',
    service_name    TEXT,
    start_time_ns   INTEGER,
    end_time_ns     INTEGER,
    duration_ns     INTEGER,
    event_count     INTEGER NOT NULL DEFAULT 0,
    link_count      INTEGER NOT NULL DEFAULT 0,
    attributes_json TEXT NOT NULL DEFAULT '{}',
    resource_json   TEXT NOT NULL DEFAULT '{}',
    scope_json      TEXT NOT NULL DEFAULT '{}',
    raw_json        TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (trace_id, span_id),
    FOREIGN KEY (trace_id) REFERENCES archived_traces(trace_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_archived_traces_archived_at
    ON archived_traces (archived_at_ns DESC);
CREATE INDEX IF NOT EXISTS idx_archived_spans_trace
    ON archived_spans (trace_id, seq);

-- Durable sampler state -----------------------------------------------------

CREATE TABLE IF NOT EXISTS windows (
    trace_id        TEXT PRIMARY KEY,
    state           TEXT NOT NULL CHECK (state IN ('collecting', 'pending', 'closed')),
    first_seen      REAL NOT NULL,
    deadline        REAL NOT NULL,
    has_error       INTEGER NOT NULL DEFAULT 0,
    next_seq        INTEGER NOT NULL DEFAULT 0,
    expired_at      REAL,
    closed_at       REAL
);

CREATE INDEX IF NOT EXISTS idx_windows_state ON windows (state, closed_at);

CREATE TABLE IF NOT EXISTS staged_spans (
    trace_id        TEXT NOT NULL,
    span_id         TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    parent_span_id  TEXT,
    name            TEXT NOT NULL DEFAULT '',
    kind            TEXT NOT NULL DEFAULT 'UNSPECIFIED',
    status_code     TEXT NOT NULL DEFAULT 'UNSET',
    status_message  TEXT NOT NULL DEFAULT '',
    service_name    TEXT,
    start_time_ns   INTEGER,
    end_time_ns     INTEGER,
    duration_ns     INTEGER,
    event_count     INTEGER NOT NULL DEFAULT 0,
    link_count      INTEGER NOT NULL DEFAULT 0,
    attributes_json TEXT NOT NULL DEFAULT '{}',
    resource_json   TEXT NOT NULL DEFAULT '{}',
    scope_json      TEXT NOT NULL DEFAULT '{}',
    raw_json        TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS idx_staged_spans_trace
    ON staged_spans (trace_id, seq);
"""

_SPAN_COLUMNS = (
    " trace_id, span_id, seq, parent_span_id, name, kind,"
    " status_code, status_message, service_name,"
    " start_time_ns, end_time_ns, duration_ns, event_count,"
    " link_count, attributes_json, resource_json, scope_json, raw_json"
)
_SPAN_PLACEHOLDERS = ", ".join("?" for _ in range(18))


class ArchiveStore:
    """Thread-safe SQLite archive and staging area.

    All methods are synchronous; callers run them in a worker thread.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        directory = os.path.dirname(os.path.abspath(path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, timeout=10.0, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------- archiving
    def write_trace(
        self,
        trace_id: str,
        spans: list[SpanRecord],
        closed_at: Optional[float] = None,
    ) -> None:
        """Atomically archive a whole trace.  Raises on any failure.

        The archive rows, the removal of the trace's staged spans and the
        window's closed marker land in a single transaction, so a crash
        either leaves the window pending (recovery will archive it again) or
        leaves it closed (it can never be archived twice).
        """

        if not spans:
            raise ValueError("refusing to archive an empty trace")

        ordered = sorted(spans, key=lambda s: (s.seq, s.span_id))
        starts = [int(s.start_time_ns) for s in ordered if s.start_time_ns]
        ends = [int(s.end_time_ns) for s in ordered if s.end_time_ns]
        min_start = min(starts) if starts else None
        max_end = max(ends) if ends else None
        first_seen_ns = time.time_ns()
        duration = max_end - min_start if min_start is not None and max_end is not None else None

        roots = [s for s in ordered if s.parent_span_id is None]
        root = roots[0] if roots else ordered[0]
        services = sorted({s.service_name for s in ordered if s.service_name})
        error_count = sum(1 for s in ordered if s.is_error)

        with self._lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT OR REPLACE INTO archived_traces ("
                    " trace_id, first_seen_ns, last_span_ns, archived_at_ns,"
                    " span_count, error_count, root_name, root_span_id,"
                    " service_names, min_start_ns, max_end_ns, duration_ns"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        trace_id,
                        first_seen_ns,
                        first_seen_ns,
                        first_seen_ns,
                        len(ordered),
                        error_count,
                        root.name,
                        root.span_id,
                        json.dumps(services, ensure_ascii=False),
                        min_start,
                        max_end,
                        duration,
                    ),
                )
                conn.execute("DELETE FROM archived_spans WHERE trace_id = ?", (trace_id,))
                conn.executemany(
                    f"INSERT INTO archived_spans ({_SPAN_COLUMNS})"
                    f" VALUES ({_SPAN_PLACEHOLDERS})",
                    [self._span_row(span) for span in ordered],
                )
                # Bookkeeping in the same commit: the staged spans are gone
                # and the window can never be archived a second time.
                conn.execute("DELETE FROM staged_spans WHERE trace_id = ?", (trace_id,))
                if closed_at is not None:
                    conn.execute(
                        "INSERT INTO windows ("
                        " trace_id, state, first_seen, deadline, has_error,"
                        " next_seq, expired_at, closed_at"
                        ") VALUES (?, 'closed', ?, ?, 0, 0, NULL, ?) "
                        "ON CONFLICT(trace_id) DO UPDATE SET"
                        " state = 'closed', closed_at = excluded.closed_at",
                        (trace_id, closed_at, closed_at, closed_at),
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    # -------------------------------------------------------------- staging
    def stage_ingest(
        self, windows: list[ActiveTrace], spans: list[SpanRecord]
    ) -> None:
        """Persist one admitted ingest batch atomically.

        Every touched window row is upserted (state ``collecting``) and every
        newly admitted span is inserted.  If anything fails the whole batch is
        rolled back and the caller must not let it count.
        """

        if not windows and not spans:
            return
        with self._lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                if windows:
                    conn.executemany(
                        "INSERT INTO windows ("
                        " trace_id, state, first_seen, deadline, has_error,"
                        " next_seq, expired_at, closed_at"
                        ") VALUES (?, 'collecting', ?, ?, ?, ?, NULL, NULL) "
                        "ON CONFLICT(trace_id) DO UPDATE SET"
                        " state = 'collecting',"
                        " first_seen = excluded.first_seen,"
                        " deadline = excluded.deadline,"
                        " has_error = excluded.has_error,"
                        " next_seq = excluded.next_seq,"
                        " expired_at = NULL,"
                        " closed_at = NULL",
                        [
                            (
                                trace.trace_id,
                                float(trace.first_seen),
                                float(trace.deadline),
                                1 if trace.has_error else 0,
                                int(trace.next_seq),
                            )
                            for trace in windows
                        ],
                    )
                if spans:
                    conn.executemany(
                        f"INSERT INTO staged_spans ({_SPAN_COLUMNS})"
                        f" VALUES ({_SPAN_PLACEHOLDERS})",
                        [self._span_row(span) for span in spans],
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def expire_windows(
        self,
        pending: list[tuple[str, float]],
        discarded: list[tuple[str, float]],
    ) -> None:
        """Persist window-expiry transitions in one transaction.

        ``pending`` keeps its staged spans (they are the archive source);
        ``discarded`` windows are closed and their staged spans dropped.
        """

        if not pending and not discarded:
            return
        with self._lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                if pending:
                    conn.executemany(
                        "UPDATE windows SET state = 'pending', expired_at = ?"
                        " WHERE trace_id = ?",
                        [(expired_at, trace_id) for trace_id, expired_at in pending],
                    )
                if discarded:
                    conn.executemany(
                        "UPDATE windows SET state = 'closed', closed_at = ?"
                        " WHERE trace_id = ?",
                        [(closed_at, trace_id) for trace_id, closed_at in discarded],
                    )
                    conn.executemany(
                        "DELETE FROM staged_spans WHERE trace_id = ?",
                        [(trace_id,) for trace_id, _ in discarded],
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def prune_closed(self, cutoff: float) -> None:
        """Forget closed windows whose marker is older than ``cutoff``."""

        with self._lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "DELETE FROM windows WHERE state = 'closed'"
                    " AND closed_at IS NOT NULL AND closed_at < ?",
                    (cutoff,),
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    # -------------------------------------------------------------- recovery
    def load_windows(self) -> tuple[list[dict], list[dict]]:
        """Return ``(open_windows, closed_windows)`` for sampler recovery."""

        with self._lock:
            open_rows = self._conn.execute(
                "SELECT trace_id, state, first_seen, deadline, has_error,"
                " next_seq, expired_at"
                " FROM windows WHERE state IN ('collecting', 'pending')"
            ).fetchall()
            closed_rows = self._conn.execute(
                "SELECT trace_id, closed_at FROM windows WHERE state = 'closed'"
            ).fetchall()
        return [dict(row) for row in open_rows], [dict(row) for row in closed_rows]

    def load_staged_spans(
        self, trace_ids: Iterable[str]
    ) -> dict[str, list[SpanRecord]]:
        """Reload every staged span for the given windows, ordered by seq."""

        ids = list(dict.fromkeys(trace_ids))
        result: dict[str, list[SpanRecord]] = {trace_id: [] for trace_id in ids}
        if not ids:
            return result
        with self._lock:
            for start in range(0, len(ids), 500):
                chunk = ids[start : start + 500]
                placeholders = ", ".join("?" for _ in chunk)
                rows = self._conn.execute(
                    "SELECT * FROM staged_spans"
                    f" WHERE trace_id IN ({placeholders})"
                    " ORDER BY trace_id, seq, span_id",
                    chunk,
                ).fetchall()
                for row in rows:
                    result[row["trace_id"]].append(self._span_record_from_row(row))
        return result

    # --------------------------------------------------------------- queries
    def get_trace(self, trace_id: str) -> Optional[dict[str, Any]]:
        trace_id = trace_id.strip().lower()
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM archived_traces WHERE trace_id = ?", (trace_id,)
            ).fetchone()
            if row is None:
                return None
            span_rows = self._conn.execute(
                "SELECT * FROM archived_spans WHERE trace_id = ? ORDER BY seq, span_id",
                (trace_id,),
            ).fetchall()
        trace = dict(row)
        trace["service_names"] = json.loads(trace.get("service_names") or "[]")
        trace["spans"] = [self._span_from_row(span) for span in span_rows]
        return trace

    def list_traces(self, limit: int = 100, offset: int = 0) -> tuple[list[dict], int]:
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        with self._lock:
            total = self._conn.execute(
                "SELECT COUNT(*) FROM archived_traces"
            ).fetchone()[0]
            rows = self._conn.execute(
                "SELECT trace_id, archived_at_ns, span_count, error_count,"
                " root_name, service_names, duration_ns, min_start_ns"
                " FROM archived_traces ORDER BY archived_at_ns DESC"
                " LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        traces = []
        for row in rows:
            item = dict(row)
            item["service_names"] = json.loads(item.get("service_names") or "[]")
            traces.append(item)
        return traces, int(total)

    def count(self) -> int:
        with self._lock:
            return int(
                self._conn.execute("SELECT COUNT(*) FROM archived_traces").fetchone()[0]
            )

    # ---------------------------------------------------------------- rows
    @staticmethod
    def _span_row(span: SpanRecord) -> tuple:
        return (
            span.trace_id,
            span.span_id,
            span.seq,
            span.parent_span_id,
            span.name,
            span.kind,
            span.status_code,
            span.status_message,
            span.service_name,
            int(span.start_time_ns) if span.start_time_ns else None,
            int(span.end_time_ns) if span.end_time_ns else None,
            span.duration_ns,
            span.event_count,
            span.link_count,
            span.attributes_json,
            span.resource_json,
            span.scope_json,
            span.raw_json,
        )

    @staticmethod
    def _span_record_from_row(row: sqlite3.Row) -> SpanRecord:
        return SpanRecord(
            trace_id=row["trace_id"],
            span_id=row["span_id"],
            parent_span_id=row["parent_span_id"],
            name=row["name"],
            kind=row["kind"],
            status_code=row["status_code"],
            status_message=row["status_message"],
            is_error=row["status_code"] == "ERROR",
            start_time_ns=(
                str(row["start_time_ns"]) if row["start_time_ns"] is not None else None
            ),
            end_time_ns=(
                str(row["end_time_ns"]) if row["end_time_ns"] is not None else None
            ),
            duration_ns=row["duration_ns"],
            service_name=row["service_name"],
            event_count=row["event_count"],
            link_count=row["link_count"],
            resource_json=row["resource_json"],
            scope_json=row["scope_json"],
            attributes_json=row["attributes_json"],
            raw_json=row["raw_json"],
            seq=row["seq"],
        )

    @staticmethod
    def _span_from_row(row: sqlite3.Row) -> dict[str, Any]:
        span = dict(row)
        for key in ("attributes_json", "resource_json", "scope_json", "raw_json"):
            try:
                span[key[:-5]] = json.loads(span.pop(key) or "{}")
            except json.JSONDecodeError:
                span[key[:-5]] = {}
        span["is_error"] = span.get("status_code") == "ERROR"
        return span
