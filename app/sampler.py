"""Tail-based sampler state machine.

Rules implemented here:

* A trace's collection window opens when its first span arrives and lasts
  ``window_seconds`` (2s by default).  Spans arriving after it closes are
  rejected, even if the trace was discarded or is still waiting to be written.
* Spans are grouped per trace, de-duplicated by span id and capped at
  ``max_spans_per_trace`` distinct spans.
* At most ``capacity`` traces may occupy a slot.  An occupied slot is either an
  open trace or an error trace that has not yet been committed.  When all slots
  are taken, only *new* trace ids are refused; admitted traces still collect
  spans until their window closes.
* When a window closes without any ERROR span the trace is dropped and its slot
  is released at once.  A trace containing an ERROR span is moved to a pending
  queue; its slot is released only after the whole trace is committed to disk.
* Archiving can be paused.  Paused or failed writes keep the pending trace
  exactly as it was at expiry and are retried automatically once archiving is
  resumed (or a later attempt succeeds).

Durability (surviving a hard kill):

* Every admitted span is staged in SQLite (``staged_spans``) together with its
  window row (``windows``) *before* the ingest call reports success.  If the
  staging write fails, the whole batch is rolled back in memory and
  :class:`StagingError` is raised so the HTTP layer answers 503 — an
  unacknowledged span never occupies a slot or counts towards a window.
* Window transitions (collecting → pending / closed) are persisted first and
  applied in memory only after the write succeeds.
* The archive commit removes the staged spans and closes the window row in the
  same transaction, so a window is re-archived after a crash that happened
  before the commit and never archived twice after one that happened later.
* On startup the sampler rebuilds slots, de-dup state and pending archives
  from the database before any request is served.  Windows keep their
  original (wall-clock) deadline: windows still inside it keep collecting,
  expired ones are decided immediately by the usual ERROR rule.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field, replace
from typing import Callable, Optional

from .models import ActiveTrace, IngestResult, PendingArchive, SpanRecord
from .storage import ArchiveStore

log = logging.getLogger("tailsampler.sampler")

DEFAULT_WINDOW_SECONDS = 2.0
DEFAULT_CAPACITY = 16
DEFAULT_MAX_SPANS = 64
# How long a closed trace id is remembered so late spans are refused.  Bounded
# by both time and entry count to keep memory finite under load.
CLOSED_RETENTION_SECONDS = float(os.environ.get("CLOSED_TRACE_RETENTION", "300"))
CLOSED_MEMORY = int(os.environ.get("CLOSED_TRACE_MEMORY", "200000"))
# Closed window rows are housekeeping; the database prune runs at most once
# per this many (clock) seconds instead of on every tick.
_PRUNE_INTERVAL_SECONDS = 30.0


class StagingError(RuntimeError):
    """The ingest batch could not be persisted; nothing was admitted."""


@dataclass
class _Batch:
    """Bookkeeping for one ingest call so a failed staging write rolls back."""

    staged_spans: list[SpanRecord] = field(default_factory=list)
    # trace id -> ActiveTrace copy taken before this batch first mutated it
    # (None when the window did not exist before the batch).
    touched: dict[str, Optional[ActiveTrace]] = field(default_factory=dict)
    closed_prior: dict[str, Optional[float]] = field(default_factory=dict)
    admitted: int = 0
    capacity_rejected: int = 0


class TraceSampler:
    def __init__(
        self,
        store: ArchiveStore,
        window_seconds: float = DEFAULT_WINDOW_SECONDS,
        capacity: int = DEFAULT_CAPACITY,
        max_spans_per_trace: int = DEFAULT_MAX_SPANS,
        tick_seconds: float = 0.05,
        clock: Optional[Callable[[], float]] = None,
    ) -> None:
        if capacity <= 0 or max_spans_per_trace <= 0 or window_seconds <= 0:
            raise ValueError("sampler limits must be positive")
        self.store = store
        self.window_seconds = float(window_seconds)
        self.capacity = int(capacity)
        self.max_spans_per_trace = int(max_spans_per_trace)
        self.tick_seconds = max(0.005, float(tick_seconds))
        # Wall clock by default so window deadlines survive a restart.
        # Tests inject a fake clock; it must stay consistent across the
        # samplers that share one database.
        self.clock: Callable[[], float] = clock or time.time

        self._lock = threading.RLock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

        self._active: dict[str, ActiveTrace] = {}
        self._pending: "OrderedDict[str, PendingArchive]" = OrderedDict()
        self._closed: "OrderedDict[str, float]" = OrderedDict()
        self._paused = False

        self._next_attempt: dict[str, float] = {}
        self._retry_delay: dict[str, float] = {}
        # Protects against the worker thread and an HTTP-triggered tick
        # writing the same pending trace concurrently.
        self._writing: set[str] = set()
        self._last_db_prune = 0.0

        self.totals = {
            "spans_accepted": 0,
            "spans_rejected": 0,
            "spans_duplicate": 0,
            "traces_admitted": 0,
            "traces_discarded": 0,
            "traces_archived": 0,
            "traces_write_failed": 0,
            "traces_capacity_rejected": 0,
        }
        self.last_error: Optional[str] = None

        # Rebuild slots, de-dup state and pending archives from the volume
        # before the sampler accepts anything.
        self._recover()

    # ------------------------------------------------------------------ life
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="trace-sampler", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=timeout)
            self._thread = None

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:  # pragma: no cover - defensive
                log.exception("sampler tick failed")
            self._wake.wait(self.tick_seconds)
            self._wake.clear()

    # --------------------------------------------------------------- control
    @property
    def paused(self) -> bool:
        with self._lock:
            return self._paused

    def pause(self) -> None:
        with self._lock:
            self._paused = True

    def resume(self) -> None:
        with self._lock:
            self._paused = False
        self._wake.set()

    # ---------------------------------------------------------------- ingest
    def ingest(self, spans: list[SpanRecord], invalid: int = 0) -> IngestResult:
        """Feed parsed spans through the sampler.

        Every span in the batch is evaluated independently: one rejected span
        (or trace) never prevents the remaining valid spans from being kept.
        Newly admitted spans are staged in SQLite together with their window
        state before this call reports success; if the staging write fails the
        whole batch is rolled back and :class:`StagingError` is raised.
        """

        result = IngestResult()
        if invalid:
            result.rejected += invalid
            result.reasons["invalid_span"] = invalid

        with self._lock:
            now = self.clock()
            batch = _Batch()
            try:
                self._expire_locked(now)
                for span in spans:
                    self._ingest_one_locked(span, now, result, batch)
                touched = [self._active[trace_id] for trace_id in batch.touched]
                if touched or batch.staged_spans:
                    self.store.stage_ingest(touched, batch.staged_spans)
            except Exception as exc:
                # Nothing in this batch may count: restore the exact
                # pre-batch state so no slot is taken and no span is
                # remembered (a retried span is not a duplicate).
                for trace_id, prior in batch.touched.items():
                    if prior is None:
                        self._active.pop(trace_id, None)
                    else:
                        self._active[trace_id] = prior
                for trace_id, prior in batch.closed_prior.items():
                    if prior is None:
                        self._closed.pop(trace_id, None)
                    else:
                        self._closed[trace_id] = prior
                raise StagingError(f"failed to stage ingest batch: {exc}") from exc

            self.totals["spans_accepted"] += result.accepted
            self.totals["spans_rejected"] += result.rejected
            self.totals["spans_duplicate"] += result.duplicates
            self.totals["traces_admitted"] += batch.admitted
            self.totals["traces_capacity_rejected"] += batch.capacity_rejected

        if self._has_work():
            self._wake.set()
        return result

    def _ingest_one_locked(
        self, span: SpanRecord, now: float, result: IngestResult, batch: _Batch
    ) -> None:
        trace_id = span.trace_id
        active = self._active.get(trace_id)

        if active is not None:
            if now >= active.deadline:
                # The window closed just now; expire it and fall through so it
                # is rejected as a closed trace instead of starting a new one.
                self._expire_locked(now)
                active = None
                result.reject("window_closed")
                return
            if span.span_id in active.spans:
                result.duplicates += 1
                return
            if len(active.spans) >= self.max_spans_per_trace:
                result.reject("span_limit")
                return
            self._backup_trace_locked(batch, active)
            span = replace(span, seq=active.next_seq)
            active.next_seq += 1
            active.spans[span.span_id] = span
            active.has_error = active.has_error or span.is_error
            batch.staged_spans.append(span)
            result.accepted += 1
            result.accepted_new += 1
            return

        if active is None and (trace_id in self._closed or trace_id in self._pending):
            result.reject("window_closed")
            return

        # New trace: may we take a slot?
        if self._occupied_locked() >= self.capacity:
            result.reject("capacity")
            batch.capacity_rejected += 1
            return

        if trace_id not in batch.closed_prior:
            batch.closed_prior[trace_id] = self._closed.get(trace_id)
        batch.touched.setdefault(trace_id, None)  # no window before this batch
        span = replace(span, seq=0)
        trace = ActiveTrace(
            trace_id=trace_id,
            deadline=now + self.window_seconds,
            first_seen=now,
            spans={span.span_id: span},
            has_error=span.is_error,
            next_seq=1,
        )
        self._active[trace_id] = trace
        self._closed.pop(trace_id, None)
        batch.admitted += 1
        batch.staged_spans.append(span)
        result.accepted += 1
        result.accepted_new += 1

    def _backup_trace_locked(self, batch: _Batch, active: ActiveTrace) -> None:
        if active.trace_id not in batch.touched:
            batch.touched[active.trace_id] = ActiveTrace(
                trace_id=active.trace_id,
                deadline=active.deadline,
                first_seen=active.first_seen,
                spans=dict(active.spans),
                has_error=active.has_error,
                next_seq=active.next_seq,
            )

    def _occupied_locked(self) -> int:
        return len(self._active) + len(self._pending)

    def _mark_closed_locked(self, trace_id: str, now: float) -> None:
        self._closed[trace_id] = now
        self._closed.move_to_end(trace_id)
        while len(self._closed) > CLOSED_MEMORY:
            self._closed.popitem(last=False)

    def _prune_closed_locked(self, now: float) -> None:
        cutoff = now - CLOSED_RETENTION_SECONDS
        while self._closed:
            oldest_id, closed_at = next(iter(self._closed.items()))
            if closed_at > cutoff:
                break
            self._closed.popitem(last=False)
        if now - self._last_db_prune >= _PRUNE_INTERVAL_SECONDS:
            self._last_db_prune = now
            try:
                self.store.prune_closed(cutoff)
            except Exception:  # housekeeping must not break ingest
                log.exception("failed to prune closed windows")

    # --------------------------------------------------------------- expiry
    def _expire_locked(self, now: float) -> None:
        expired = [
            (trace_id, trace)
            for trace_id, trace in self._active.items()
            if now >= trace.deadline
        ]
        if expired:
            pending_rows: list[tuple[str, float]] = []
            discarded_rows: list[tuple[str, float]] = []
            for trace_id, trace in expired:
                if trace.has_error and trace.spans:
                    pending_rows.append((trace_id, now))
                else:
                    discarded_rows.append((trace_id, now))
            # Persist the transitions first.  If the write fails, in-memory
            # state is left untouched and expiry is retried on the next tick.
            self.store.expire_windows(pending_rows, discarded_rows)
            for trace_id, trace in expired:
                del self._active[trace_id]
                self._mark_closed_locked(trace_id, now)
                if not trace.has_error or not trace.spans:
                    self.totals["traces_discarded"] += 1
                    continue
                # Keep the expired snapshot and, with it, the slot.  The slot
                # is freed only when ArchiveStore.write_trace commits.
                spans = sorted(trace.spans.values(), key=lambda s: (s.seq, s.span_id))
                self._pending[trace_id] = PendingArchive(
                    trace_id=trace_id, spans=spans, expired_at=now
                )
                self._next_attempt[trace_id] = now
                self._retry_delay[trace_id] = 0.0
        self._prune_closed_locked(now)

    # -------------------------------------------------------------- recovery
    def _recover(self) -> None:
        """Rebuild slots, de-dup state and pending archives from the volume."""

        with self._lock:
            now = self.clock()
            open_rows, closed_rows = self.store.load_windows()
            cutoff = now - CLOSED_RETENTION_SECONDS
            for row in closed_rows:
                closed_at = row.get("closed_at")
                if closed_at is not None and closed_at >= cutoff:
                    self._closed[row["trace_id"]] = closed_at

            staged = self.store.load_staged_spans(
                [row["trace_id"] for row in open_rows]
            )
            orphaned: list[str] = []
            for row in open_rows:
                trace_id = row["trace_id"]
                spans = staged.get(trace_id, [])
                if row["state"] == "pending":
                    if not spans:
                        # Nothing left to re-archive; close it out.
                        orphaned.append(trace_id)
                        continue
                    self._pending[trace_id] = PendingArchive(
                        trace_id=trace_id,
                        spans=spans,
                        expired_at=row.get("expired_at") or now,
                    )
                    # Retry promptly: the write never committed before the
                    # previous process died.
                    self._next_attempt[trace_id] = now
                    self._retry_delay[trace_id] = 0.0
                else:
                    self._active[trace_id] = ActiveTrace(
                        trace_id=trace_id,
                        deadline=float(row["deadline"]),
                        first_seen=float(row["first_seen"]),
                        spans={span.span_id: span for span in spans},
                        has_error=bool(row["has_error"]),
                        next_seq=int(row["next_seq"]),
                    )
            if orphaned:
                try:
                    self.store.expire_windows([], [(tid, now) for tid in orphaned])
                except Exception:
                    log.exception("failed to close orphaned windows")
                for trace_id in orphaned:
                    self._mark_closed_locked(trace_id, now)
            if self._active:
                # Windows whose deadline passed while the process was down are
                # decided right away by the usual ERROR rule.
                self._expire_locked(now)
            if self._active or self._pending or self._closed:
                log.info(
                    "recovered %d collecting / %d pending window(s), %d closed id(s)",
                    len(self._active),
                    len(self._pending),
                    len(self._closed),
                )

    # ---------------------------------------------------------------- worker
    def _has_work(self) -> bool:
        with self._lock:
            now = self.clock()
            return bool(self._pending) or any(
                now >= t.deadline for t in self._active.values()
            )

    def tick(self) -> None:
        """Expire open traces and flush pending archives that are due."""

        with self._lock:
            self._expire_locked(self.clock())

        while True:
            with self._lock:
                if self._paused:
                    break
                now = self.clock()
                due = [
                    item
                    for trace_id, item in self._pending.items()
                    if self._next_attempt.get(trace_id, 0.0) <= now
                    and trace_id not in self._writing
                ]
                for item in due:
                    self._writing.add(item.trace_id)
            if not due:
                break
            for item in due:
                try:
                    # Stop early if archiving was paused between writes.
                    if self._paused:
                        break
                    self._write_pending(item)
                finally:
                    with self._lock:
                        self._writing.discard(item.trace_id)

    def _write_pending(self, item: PendingArchive) -> None:
        try:
            self.store.write_trace(item.trace_id, item.spans, closed_at=self.clock())
        except Exception as exc:
            with self._lock:
                item.attempts += 1
                item.last_error = f"{type(exc).__name__}: {exc}"
                self.last_error = item.last_error
                self.totals["traces_write_failed"] += 1
                delay = min(30.0, max(0.5, self._retry_delay.get(item.trace_id, 0.0) * 2))
                self._retry_delay[item.trace_id] = delay
                self._next_attempt[item.trace_id] = self.clock() + delay
            log.warning("archive write failed for %s: %s", item.trace_id, exc)
            return

        with self._lock:
            # Commit succeeded: now the slot can be released.
            if self._pending.get(item.trace_id) is item:
                del self._pending[item.trace_id]
            self._next_attempt.pop(item.trace_id, None)
            self._retry_delay.pop(item.trace_id, None)
            self._mark_closed_locked(item.trace_id, self.clock())
            self.totals["traces_archived"] += 1
        log.info("archived trace %s (%d spans)", item.trace_id, len(item.spans))

    # ----------------------------------------------------------------- stats
    def snapshot(self) -> dict:
        with self._lock:
            now = self.clock()
            active = []
            for trace_id, trace in self._active.items():
                active.append(
                    {
                        "traceId": trace_id,
                        "spanCount": len(trace.spans),
                        "hasError": trace.has_error,
                        "remainingMs": max(0.0, round((trace.deadline - now) * 1000, 1)),
                    }
                )
            pending = [
                {
                    "traceId": item.trace_id,
                    "spanCount": len(item.spans),
                    "attempts": item.attempts,
                    "lastError": item.last_error,
                }
                for item in self._pending.values()
            ]
            return {
                "paused": self._paused,
                "capacity": self.capacity,
                "slotsUsed": self._occupied_locked(),
                "activeTraces": sorted(active, key=lambda t: t["traceId"]),
                "pendingTraces": pending,
                "windowSeconds": self.window_seconds,
                "maxSpansPerTrace": self.max_spans_per_trace,
                "lastError": self.last_error,
                "totals": dict(self.totals),
            }
