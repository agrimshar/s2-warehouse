"""Exit 99 (Airflow skip) when the manifest has no scenes in the interval."""

import argparse
import os
import sys
import tempfile
from pathlib import Path

import pandas as pd

from s2warehouse.metrics import record
from s2warehouse.storage import MANIFEST_KEY, download


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", required=True)
    p.add_argument("--end", required=True)
    args = p.parse_args()

    with tempfile.TemporaryDirectory() as tmp:
        download(MANIFEST_KEY, Path(tmp) / "manifest.parquet")
        df = pd.read_parquet(Path(tmp) / "manifest.parquet")
    start = pd.Timestamp(args.start, tz="UTC")
    end = pd.Timestamp(args.end, tz="UTC")
    n = int(((df["datetime"] >= start) & (df["datetime"] < end)).sum())
    print(f"{n} scenes in [{args.start}, {args.end})")
    record("gate", 0.0, "skip" if n == 0 else "ok", start=args.start, end=args.end, scenes=n)
    if n == 0:
        sys.stdout.flush()
        os._exit(99)


if __name__ == "__main__":
    main()