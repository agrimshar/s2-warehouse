"""One JSON line per stage run, appended to data/metrics/stage_runs.jsonl."""

import json
import os
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from s2warehouse.paths import DATA

METRICS = DATA / "metrics" / "stage_runs.jsonl"


def record(stage: str, seconds: float, status: str, path: Path | None = None, **counts) -> None:
    path = path or METRICS
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "ts": datetime.now(tz=UTC).isoformat(timespec="seconds"),
        "run_id": os.environ.get("AIRFLOW_CTX_DAG_RUN_ID", "manual"),
        "stage": stage,
        "seconds": round(seconds, 1),
        "status": status,
        **counts,
    }
    with path.open("a") as f:
        f.write(json.dumps(row) + "\n")


@contextmanager
def timed(stage: str, **counts) -> Iterator[dict]:
    """Time a stage. The yielded dict collects counts; recorded on exit, ok or error."""
    t0 = time.perf_counter()
    out: dict = dict(counts)
    status = "error"
    try:
        yield out
        status = "ok"
    finally:
        record(stage, time.perf_counter() - t0, status, **out)