"""Repo-anchored paths, so commands work from any working directory."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
SILVER = DATA / "delta" / "silver"
STAGING = DATA / "staging" / "silver"
WAREHOUSE = DATA / "warehouse.duckdb"
DOCS = ROOT / "docs"