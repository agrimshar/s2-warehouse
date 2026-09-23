"""Inspect one scene: zeros per bronze band, and zeros in the source COG."""

import argparse

import numpy as np
import pandas as pd
import rasterio

from s2warehouse.raster import GDAL_ENV
from s2warehouse.silver import read_s3_band
from s2warehouse.storage import bronze_key


def zero_lines(a: np.ndarray) -> str:
    idx = np.flatnonzero((a == 0).all(axis=1))
    return "none" if idx.size == 0 else f"{idx.size} ({idx.min()}..{idx.max()})"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("scene_id")
    args = p.parse_args()

    for band in ["B03", "B04", "B08", "SCL"]:
        a = read_s3_band(bronze_key(args.scene_id, band))
        print(
            f"bronze {band}: {100 * (a == 0).mean():5.1f}% zeros; "
            f"all-zero rows {zero_lines(a)}; all-zero cols {zero_lines(a.T)}"
        )

    row = pd.read_parquet("manifest.parquet").set_index("scene_id").loc[args.scene_id]
    for band in ["B03", "B04", "B08"]:
        with rasterio.Env(**GDAL_ENV), rasterio.open(row[f"href_{band}"]) as src:
            a = src.read(1, out_shape=(1098, 1098))
        print(f"source {band} (1/10 overview): {100 * (a == 0).mean():5.1f}% zeros")


if __name__ == "__main__":
    main()