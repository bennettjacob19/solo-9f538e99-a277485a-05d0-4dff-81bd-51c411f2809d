# OTLP Tail Sampler

A FastAPI + SQLite tail-sampling collector for OTLP/HTTP JSON traces. It keeps a
trace only when an ERROR span arrives during that trace's two-second collection
window, then archives the whole trace and exposes it on a small web UI.

## The rule that matters

A successful root span can be exported long before the failing child span shows
up. This service therefore cannot decide on the first span:

1. The first span of a trace opens a **2 second window** and takes one of 16
   trace slots.
2. Spans are grouped by `traceId`, de-duplicated by `spanId`, and capped at 64
   distinct spans per trace.
3. When the window closes the trace is decided:
   - **no ERROR span → dropped**, the slot is released immediately;
   - **at least one ERROR span → the entire trace is written to SQLite** in one
     transaction, and the slot is released only after the commit succeeds.
4. Spans for a trace whose window has closed are rejected, even if the trace is
   still waiting to be written.
5. At 16 occupied slots only *new* trace ids are refused; traces already
   admitted keep collecting until their own window closes.

Slots count both open traces and error traces that have expired but not yet
been committed, so the number of traces occupying the budget never exceeds the
configured capacity.

### Partial success

A rejected span never poisons the rest of the batch. Valid, in-window spans are
stored normally and the OTLP response reports the rejection count:

```json
{
  "partialSuccess": {
    "rejectedSpans": 2,
    "acceptedSpans": 5,
    "errorMessage": "rejected 2 span(s): capacity=1, window_closed=1"
  }
}
```

Rejection reasons: `capacity`, `window_closed`, `span_limit`, `invalid_span`.
A fully accepted export returns `{}` per the OTLP spec.

### Durability and crash recovery

A batch is acknowledged only after every admitted span and its window state
have been committed to SQLite (`staged_spans` + `windows` tables, next to the
archive). If that staging write fails, the whole batch is rolled back — no
window is opened or extended, no slot is taken — and the export receives
**HTTP 503** so the exporter can retry.

If the process is killed (`kill -9`, OOM, node reboot) and restarted on the
same volume, the sampler rebuilds its state from the database before serving
requests:

* **Collecting windows** keep their original deadline and every acknowledged
  span: they keep collecting, de-duplication still applies, and spans arriving
  after the original deadline are rejected as usual.
* **Windows that expired while the process was down** are decided on boot by
  the usual rule — archived if they contain an ERROR span, dropped otherwise.
* **Traces waiting to be written** are archived automatically after the
  restart. The archive commit, the removal of the staged spans and the
  window's closed marker land in one transaction, so a window is re-written
  after a crash that happened before the commit and is never archived twice
  after one that happened later.
* **Recently closed trace ids** stay rejected for late spans.

The overview page reflects the recovered state: occupied slots, traces still
collecting, writes still pending, and every archive already on the volume.

### Pause / resume

Archiving can be paused from the page or via `POST /api/archive/pause`. Traces
that expire while paused keep exactly the spans they held at expiry (later spans
are still refused), stay in memory, and stay invisible on the page. When
archiving resumes — or when a failed SQLite write later succeeds — every pending
trace is written automatically. Write failures use exponential backoff (max
30s) and never lose the expired content.

## Run with Docker Compose

```bash
docker compose up --build
# open http://localhost:8000
```

Data is persisted in the `sampler-data` volume. Killing or replacing the
container and starting it again on the same volume resumes every in-flight
window and pending write exactly where it stopped (see *Durability and crash
recovery* above). Environment overrides can be set in a `.env` file next to
`docker-compose.yml`:

| Variable | Default | Meaning |
| --- | --- | --- |
| `PORT` | `8000` | host port |
| `SAMPLE_WINDOW_SECONDS` | `2` | collection window per trace |
| `MAX_ACTIVE_TRACES` | `16` | traces occupying slots |
| `MAX_SPANS_PER_TRACE` | `64` | distinct spans kept per trace |
| `DB_PATH` | `/data/traces.db` | SQLite file (container) |
| `LOG_LEVEL` | `INFO` | log level |

## Run locally

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn app.main:app --reload
pytest -q
```

## Sending traces

```bash
curl -s http://localhost:8000/v1/traces \
  -H 'Content-Type: application/json' \
  -d '{
    "resourceSpans": [{
      "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "checkout"}}]},
      "scopeSpans": [{"scope": {"name": "demo"}, "spans": [{
        "traceId": "0123456789abcdef0123456789abcdef",
        "spanId": "0123456789abcdef",
        "name": "POST /checkout",
        "kind": 2,
        "startTimeUnixNano": "1700000000000000000",
        "endTimeUnixNano": "1700000000500000000",
        "status": {"code": 2, "message": "payment declined"}
      }]}]
    }]
  }'
```

Only OTLP/HTTP JSON is decoded; protobuf exports receive HTTP 415.

## HTTP endpoints

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/v1/traces` | OTLP/HTTP JSON export |
| `GET` | `/` | overview: status, archived traces, pause/resume |
| `GET` | `/traces/{traceId}` | every archived span of one trace |
| `GET` | `/api/status` | sampler state and counters |
| `GET` | `/api/traces` | archived traces (`limit`, `offset`) |
| `GET` | `/api/traces/{traceId}` | archived trace as JSON |
| `POST` | `/api/archive/pause` · `/api/archive/resume` | pause / resume archiving |
| `GET` | `/healthz` | liveness |

## Layout

```
app/otlp.py      OTLP/HTTP JSON parsing and response shaping
app/sampler.py   window/slot state machine, dedupe, cap, pause, retry, recovery
app/storage.py   transactional SQLite archive + durable ingest staging
app/main.py      FastAPI app factory, routes, templates
tests/           sampler unit tests, HTTP-level tests, crash-recovery tests
```
