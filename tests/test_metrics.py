import json

from s2warehouse.metrics import record, timed


def test_record_appends_json_line(tmp_path):
    p = tmp_path / "m.jsonl"
    record("ingest", 12.34, "ok", path=p, scenes=3)
    record("ingest", 1.0, "error", path=p)
    rows = [json.loads(ln) for ln in p.read_text().splitlines()]
    assert [r["status"] for r in rows] == ["ok", "error"]
    assert rows[0]["scenes"] == 3 and rows[0]["seconds"] == 12.3
    assert rows[0]["run_id"] == "manual"


def test_timed_records_error_on_exception(tmp_path, monkeypatch):
    import s2warehouse.metrics as m

    monkeypatch.setattr(m, "METRICS", tmp_path / "m.jsonl")
    try:
        with timed("x") as out:
            out["rows"] = 1
            raise ValueError("boom")
    except ValueError:
        pass
    row = json.loads((tmp_path / "m.jsonl").read_text())
    assert row["status"] == "error" and row["rows"] == 1