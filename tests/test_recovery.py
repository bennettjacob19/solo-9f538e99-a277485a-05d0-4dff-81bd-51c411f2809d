"""Crash-recovery tests: a killed collector must resume from its SQLite volume.

A "crash" here is simulated by abandoning the sampler and its store (no
shutdown hooks, in-memory state lost) and building fresh ones on the same
database file with the same clock.
"""

from __future__ import annotations

import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402
from app.sampler import StagingError, TraceSampler  # noqa: E402
from app.storage import ArchiveStore  # noqa: E402
from tests.test_api import otlp_payload, post, sid, tid  # noqa: E402
from tests.test_sampler import FakeClock, span, trace_id  # noqa: E402

import json  # noqa: E402

KW = dict(window_seconds=2.0, capacity=16, max_spans_per_trace=64)


@pytest.fixture()
def clock():
    return FakeClock(start=1000.0)


class Rig:
    """One collector process; crash()/restart() cycle it on the same volume."""

    def __init__(self, path: str, clock, **kwargs) -> None:
        self.path = path
        self.clock = clock
        self.kwargs = kwargs
        self.store = None
        self.sampler = None
        self.boot()

    def boot(self) -> None:
        self.store = ArchiveStore(self.path)
        self.sampler = TraceSampler(self.store, clock=self.clock, **self.kwargs)

    def crash(self) -> None:
        """SIGKILL: in-memory state vanishes, only committed SQLite rows stay."""
        self.store.close()
        self.store = None
        self.sampler = None

    def restart(self) -> None:
        self.boot()

    def close(self) -> None:
        if self.store is not None:
            self.store.close()


@pytest.fixture()
def rig(tmp_path, clock):
    rig = Rig(str(tmp_path / "traces.db"), clock, **KW)
    yield rig
    rig.close()


def test_collecting_window_survives_restart(rig, clock):
    tid_ = trace_id()
    sid1 = "1" * 16
    rig.sampler.ingest([span(tid_, "root", sid=sid1)])
    assert rig.sampler.snapshot()["slotsUsed"] == 1
    rig.crash()

    clock.advance(1.0)  # still inside the original 2s window
    rig.restart()
    snap = rig.sampler.snapshot()
    assert snap["slotsUsed"] == 1
    assert [t["traceId"] for t in snap["activeTraces"]] == [tid_]
    assert snap["activeTraces"][0]["spanCount"] == 1

    # De-dup state was restored: the acknowledged span is a duplicate now.
    again = rig.sampler.ingest([span(tid_, "root", sid=sid1)])
    assert again.duplicates == 1 and again.accepted == 0
    # The window keeps collecting new spans until its original deadline.
    assert rig.sampler.ingest([span(tid_, "child", error=True)]).accepted == 1

    clock.advance(1.0)  # the original deadline is reached
    late = rig.sampler.ingest([span(tid_, "late")])
    assert late.reasons == {"window_closed": 1}
    rig.sampler.tick()
    archived = rig.store.get_trace(tid_)
    assert {s["name"] for s in archived["spans"]} == {"root", "child"}


def test_pending_archive_is_rewritten_after_restart(rig, clock):
    tid_ = trace_id()
    rig.sampler.ingest([span(tid_, "root"), span(tid_, "pay", error=True)])
    rig.sampler.pause()
    clock.advance(2.0)
    rig.sampler.tick()  # expired, but the write is paused -> stays pending
    assert rig.store.count() == 0
    rig.crash()  # killed before the archive commit

    rig.restart()
    snap = rig.sampler.snapshot()
    assert snap["slotsUsed"] == 1
    assert [p["traceId"] for p in snap["pendingTraces"]] == [tid_]
    assert snap["pendingTraces"][0]["spanCount"] == 2

    rig.sampler.tick()  # the recovered pending trace is written automatically
    assert rig.store.count() == 1
    archived = rig.store.get_trace(tid_)
    assert {s["name"] for s in archived["spans"]} == {"root", "pay"}
    assert rig.sampler.snapshot()["slotsUsed"] == 0


def test_committed_archive_is_never_rewritten(rig, clock):
    tid_ = trace_id()
    rig.sampler.ingest([span(tid_, "err", error=True)])
    clock.advance(2.0)
    rig.sampler.tick()
    assert rig.store.count() == 1
    rig.crash()  # killed after the commit

    rig.restart()
    assert rig.sampler.snapshot()["slotsUsed"] == 0
    rig.sampler.tick()
    rig.sampler.tick()
    assert rig.store.count() == 1  # not archived twice
    assert rig.sampler.snapshot()["totals"]["traces_archived"] == 0
    # The closed marker survived, so late spans are still rejected.
    assert rig.sampler.ingest([span(tid_, "late")]).reasons == {"window_closed": 1}


def test_discarded_trace_stays_discarded(rig, clock):
    tid_ = trace_id()
    rig.sampler.ingest([span(tid_)])
    clock.advance(2.0)
    rig.sampler.tick()
    assert rig.store.count() == 0
    rig.crash()

    rig.restart()
    assert rig.sampler.snapshot()["slotsUsed"] == 0
    assert rig.sampler.ingest([span(tid_)]).reasons == {"window_closed": 1}
    assert rig.store.count() == 0


def test_windows_expiring_during_downtime_are_decided_on_boot(rig, clock):
    err_tid, ok_tid = trace_id(), trace_id()
    rig.sampler.ingest([span(err_tid, "err", error=True), span(ok_tid, "ok")])
    rig.crash()  # both windows were still collecting when the process died
    clock.advance(5.0)  # both deadlines pass while it is down

    rig.restart()  # recovery decides both immediately
    snap = rig.sampler.snapshot()
    assert [p["traceId"] for p in snap["pendingTraces"]] == [err_tid]
    rig.sampler.tick()
    assert rig.store.count() == 1
    assert rig.store.get_trace(err_tid)["span_count"] == 1
    assert rig.store.get_trace(ok_tid) is None
    # The discarded id is closed for late spans.
    assert rig.sampler.ingest([span(ok_tid)]).reasons == {"window_closed": 1}


def test_slots_and_capacity_survive_restart(tmp_path, clock):
    rig = Rig(str(tmp_path / "traces.db"), clock, window_seconds=2.0, capacity=2,
              max_spans_per_trace=64)
    try:
        a, b, c = trace_id(), trace_id(), trace_id()
        rig.sampler.ingest([span(a), span(b)])
        rig.crash()

        rig.restart()
        assert rig.sampler.snapshot()["slotsUsed"] == 2
        blocked = rig.sampler.ingest([span(c)])
        assert blocked.reasons == {"capacity": 1}
        # Admitted windows keep collecting after the restart.
        assert rig.sampler.ingest([span(a, "more")]).accepted == 1
    finally:
        rig.close()


class StageFailingStore(ArchiveStore):
    """Store whose ingest staging fails while ``fail`` is True."""

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.fail = True
        self.attempts = 0

    def stage_ingest(self, windows, spans):
        self.attempts += 1
        if self.fail:
            raise RuntimeError("staging disk full")
        return super().stage_ingest(windows, spans)


def test_failed_staging_admits_nothing(tmp_path, clock):
    store = StageFailingStore(str(tmp_path / "traces.db"))
    try:
        sampler = TraceSampler(store, clock=clock, **KW)
        tid_ = trace_id()
        with pytest.raises(StagingError):
            sampler.ingest([span(tid_, "root", sid="1" * 16)])
        # The batch did not count: no slot, no window, no de-dup memory.
        snap = sampler.snapshot()
        assert snap["slotsUsed"] == 0
        assert snap["totals"]["spans_accepted"] == 0
        assert snap["totals"]["traces_admitted"] == 0

        # After the failure is fixed the same span is admitted as new,
        # not reported as a duplicate.
        store.fail = False
        result = sampler.ingest([span(tid_, "root", sid="1" * 16)])
        assert result.accepted == 1 and result.duplicates == 0
        assert sampler.snapshot()["slotsUsed"] == 1
    finally:
        store.close()


def test_failed_staging_rolls_back_the_whole_batch(tmp_path, clock):
    store = StageFailingStore(str(tmp_path / "traces.db"))
    try:
        sampler = TraceSampler(store, clock=clock, **KW)
        a, b = trace_id(), trace_id()
        store.fail = False
        sampler.ingest([span(a, "a0", sid="a" * 16)])

        store.fail = True
        with pytest.raises(StagingError):
            sampler.ingest(
                [
                    span(a, "a1", sid="b" * 16, error=True),  # extends window a
                    span(b, "b0", sid="c" * 16),  # opens window b
                ]
            )
        snap = sampler.snapshot()
        assert snap["slotsUsed"] == 1  # b never took a slot
        assert snap["activeTraces"][0]["spanCount"] == 1  # a1 never counted

        store.fail = False
        result = sampler.ingest(
            [span(a, "a1", sid="b" * 16, error=True), span(b, "b0", sid="c" * 16)]
        )
        assert result.accepted == 2 and result.duplicates == 0
        assert sampler.snapshot()["slotsUsed"] == 2
    finally:
        store.close()


# ---------------------------------------------------------------------------
# HTTP level
# ---------------------------------------------------------------------------


def boot_app(db_path, clock):
    return create_app(
        db_path,
        window_seconds=2.0,
        capacity=16,
        max_spans_per_trace=64,
        start_sampler_thread=False,
        clock=clock,
    )


def test_ingest_returns_503_when_staging_fails(tmp_path, clock, monkeypatch):
    from app import storage

    original = storage.ArchiveStore.stage_ingest

    def boom(self, windows, spans):
        raise RuntimeError("read-only database")

    app = boot_app(str(tmp_path / "traces.db"), clock)
    with TestClient(app) as client:
        monkeypatch.setattr(storage.ArchiveStore, "stage_ingest", boom)
        response = post(client, [{"trace": tid(1), "span": sid(1)}])
        assert response.status_code == 503
        assert "read-only database" in response.json()["partialSuccess"]["errorMessage"]
        # Nothing was admitted: no slot is occupied.
        assert client.get("/api/status").json()["slotsUsed"] == 0

        monkeypatch.setattr(storage.ArchiveStore, "stage_ingest", original)
        response = post(client, [{"trace": tid(1), "span": sid(1)}])
        assert response.status_code == 200
        assert response.json() == {}
        assert client.get("/api/status").json()["slotsUsed"] == 1
    app.state.store.close()


def test_http_restart_recovers_pending_trace(tmp_path, clock):
    db = str(tmp_path / "traces.db")
    trace = tid(7)
    app1 = boot_app(db, clock)
    with TestClient(app1) as client1:
        response = post(
            client1,
            [
                {"trace": trace, "span": sid(1), "name": "POST /checkout"},
                {
                    "trace": trace,
                    "span": sid(2),
                    "parent": sid(1),
                    "name": "POST /pay",
                    "error": True,
                },
            ],
        )
        assert response.status_code == 200
        assert client1.get("/api/status").json()["slotsUsed"] == 1
    # The process is killed; the deadline passes while it is down.
    clock.advance(3.0)

    app2 = boot_app(db, clock)
    with TestClient(app2) as client2:
        status = client2.get("/api/status").json()
        assert status["slotsUsed"] == 1
        assert [p["traceId"] for p in status["pendingTraces"]] == [trace]
        # The overview shows the restored occupancy and pending count.
        assert "1 / 16" in client2.get("/").text

        app2.state.sampler.tick()  # flush the recovered pending write
        assert client2.get("/api/status").json()["archivedTraces"] == 1
        detail = client2.get(f"/traces/{trace}")
        assert detail.status_code == 200
        assert "POST /pay" in detail.text
        assert "payment declined" in detail.text
    app2.state.store.close()


def test_http_restart_keeps_collecting_window_and_dedup(tmp_path, clock):
    db = str(tmp_path / "traces.db")
    trace = tid(8)
    app1 = boot_app(db, clock)
    with TestClient(app1) as client1:
        assert post(client1, [{"trace": trace, "span": sid(1)}]).status_code == 200
    clock.advance(1.0)  # restart one second later, window still open

    app2 = boot_app(db, clock)
    with TestClient(app2) as client2:
        # The acknowledged span is a duplicate, not a new window.
        response = client2.post(
            "/v1/traces",
            content=json.dumps(otlp_payload([{"trace": trace, "span": sid(1)}])),
            headers={"content-type": "application/json"},
        )
        assert response.status_code == 200
        assert response.json() == {}
        status = client2.get("/api/status").json()
        assert status["slotsUsed"] == 1
        assert status["totals"]["spans_duplicate"] == 1
        # New spans still collect until the original deadline.
        assert post(client2, [{"trace": trace, "span": sid(2), "error": True}]).status_code == 200
        clock.advance(1.0)
        late = post(client2, [{"trace": trace, "span": sid(3)}])
        assert "window_closed" in late.json()["partialSuccess"]["errorMessage"]
        app2.state.sampler.tick()
        assert client2.get(f"/api/traces/{trace}").json()["span_count"] == 2
    app2.state.store.close()


def test_archives_from_the_old_volume_remain_visible(tmp_path, clock):
    db = str(tmp_path / "traces.db")
    trace = tid(9)
    app1 = boot_app(db, clock)
    with TestClient(app1) as client1:
        post(client1, [{"trace": trace, "span": sid(1), "name": "boom", "error": True}])
        clock.advance(2.0)
        app1.state.sampler.tick()
        assert client1.get("/api/status").json()["archivedTraces"] == 1
    # Killed after the commit: the archive stays, nothing is written twice.
    app2 = boot_app(db, clock)
    with TestClient(app2) as client2:
        status = client2.get("/api/status").json()
        assert status["archivedTraces"] == 1
        assert status["slotsUsed"] == 0
        assert trace in client2.get("/").text
        assert "boom" in client2.get(f"/traces/{trace}").text
        # Late spans for the archived trace stay rejected after the restart.
        late = post(client2, [{"trace": trace, "span": sid(2)}])
        assert late.json()["partialSuccess"]["rejectedSpans"] == 1
        app2.state.sampler.tick()
        assert client2.get("/api/status").json()["archivedTraces"] == 1
    app2.state.store.close()
