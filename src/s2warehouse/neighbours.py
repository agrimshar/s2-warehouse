"""H3 ring-1 neighbours for every cell, as (cell_idx, neighbour_idx) pairs in S3."""

import tempfile
from pathlib import Path

import h3
import pandas as pd

from s2warehouse.grid import GRID_PREFIX
from s2warehouse.storage import download, upload


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        cells_path = Path(tmp) / "cells.parquet"
        download(f"{GRID_PREFIX}/cells.parquet", cells_path)
        cells = pd.read_parquet(cells_path)
        idx = dict(zip(cells.h3_cell, cells.cell_idx))
        pairs = [
            (i, idx[n])
            for c, i in idx.items()
            for n in h3.grid_disk(c, 1)
            if n != c and n in idx
        ]
        df = pd.DataFrame(pairs, columns=["cell_idx", "neighbour_idx"])
        out = Path(tmp) / "neighbours.parquet"
        df.to_parquet(out, index=False)
        upload(out, f"{GRID_PREFIX}/neighbours.parquet")
        print(f"{len(df)} neighbour pairs for {len(idx)} cells")


if __name__ == "__main__":
    main()