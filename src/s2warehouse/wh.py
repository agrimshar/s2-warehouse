"""Run one SQL statement against the DuckDB warehouse and print the result."""

import sys

import duckdb

from s2warehouse.paths import WAREHOUSE

DB = str(WAREHOUSE)


def main() -> None:
    con = duckdb.connect(DB)
    rel = con.sql(sys.argv[1])
    if rel is not None:
        print(rel)
    con.close()


if __name__ == "__main__":
    main()