"""Build the H3 grid for the tile: which H3 cell each 20 m pixel belongs to."""

import argparse
import tempfile
from pathlib import Path

import boto3
import h3
import numpy as np
import pandas as pd
import rasterio
from pyproj import Transformer
from rasterio.features import rasterize
from rasterio.session import AWSSession

from s2warehouse.raster import GDAL_ENV
from s2warehouse.storage import BUCKET, bronze_key, upload

H3_RES = 7
BUFFER_M = 5000
GRID_PREFIX = "grid/tile=17TPJ"


def s3_env() -> rasterio.Env:
    return rasterio.Env(session=AWSSession(boto3.Session()), **GDAL_ENV)


def read_meta(scene_id: str) -> dict:
    """The tile's raster grid, read from one bronze SCL. Every scene shares it."""
    uri = f"s3://{BUCKET}/{bronze_key(scene_id, 'SCL')}"
    with s3_env(), rasterio.open(uri) as src:
        return {"crs": src.crs, "transform": src.transform, "shape": src.shape}


def cells_covering(meta: dict, res: int = H3_RES, buffer_m: float = BUFFER_M) -> list[str]:
    """H3 cells whose centroid falls inside the tile footprint plus a buffer."""
    t = meta["transform"]
    h, w = meta["shape"]
    x0, y0 = t.c - buffer_m, t.f + buffer_m
    x1, y1 = t.c + w * t.a + buffer_m, t.f + h * t.e - buffer_m
    to_ll = Transformer.from_crs(meta["crs"], "EPSG:4326", always_xy=True)
    lons, lats = to_ll.transform([x0, x1, x1, x0], [y0, y0, y1, y1])
    poly = h3.LatLngPoly(list(zip(lats, lons)))
    return sorted(h3.polygon_to_cells(poly, res))


def rasterize_cells(cells: list[str], meta: dict) -> np.ndarray:
    """int32 raster: index of the cell each pixel centre falls in, -1 if none."""
    to_utm = Transformer.from_crs("EPSG:4326", meta["crs"], always_xy=True)
    shapes = []
    for i, cell in enumerate(cells):
        lats, lngs = zip(*h3.cell_to_boundary(cell))
        xs, ys = to_utm.transform(lngs, lats)
        ring = list(zip(xs, ys))
        ring.append(ring[0])
        shapes.append(({"type": "Polygon", "coordinates": [ring]}, i))
    return rasterize(
        shapes, out_shape=meta["shape"], transform=meta["transform"],
        fill=-1, dtype="int32",
    )


def zonal_fraction(cell_index: np.ndarray, mask: np.ndarray, valid: np.ndarray, n: int) -> np.ndarray:
    """Fraction of valid pixels per cell where mask is True. NaN with no valid pixels."""
    idx = cell_index[valid]
    hits = np.bincount(idx, weights=mask[valid], minlength=n)
    total = np.bincount(idx, minlength=n)
    out = np.full(n, np.nan)
    np.divide(hits, total, out=out, where=total > 0)
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--scene", default="S2B_17TPJ_20250704_0_L2A")
    args = p.parse_args()

    meta = read_meta(args.scene)
    print(f"tile grid: {meta['shape']} {meta['crs']} pixel={meta['transform'].a:.0f} m")

    cells = cells_covering(meta)
    n = len(cells)
    print(f"{n} H3 res-{H3_RES} cells cover the tile plus {BUFFER_M} m buffer")

    cell_index = rasterize_cells(cells, meta)
    assigned = cell_index >= 0
    print(f"pixels assigned to a cell: {100 * assigned.mean():.2f}%")

    uri = f"s3://{BUCKET}/{bronze_key(args.scene, 'SCL')}"
    with s3_env(), rasterio.open(uri) as src:
        scl = src.read(1)
    valid = assigned & (scl != 0)
    water_pct = 100 * zonal_fraction(cell_index, scl == 6, valid, n)
    pixels = np.bincount(cell_index[assigned], minlength=n)

    centroids = [h3.cell_to_latlng(c) for c in cells]
    df = pd.DataFrame({
        "cell_idx": np.arange(n),
        "h3_cell": cells,
        "lat": [c[0] for c in centroids],
        "lon": [c[1] for c in centroids],
        "area_km2": [h3.cell_area(c, unit="km^2") for c in cells],
        "pixels_in_tile": pixels,
        "water_pct": water_pct,
    })
    in_tile = df[df.pixels_in_tile > 0]
    land = in_tile[in_tile.water_pct < 90]
    print(f"cells with pixels: {len(in_tile)}   land cells (<90% water): {len(land)}")
    print(in_tile["pixels_in_tile"].describe())

    with tempfile.TemporaryDirectory() as tmp:
        tif = Path(tmp) / "cell_index.tif"
        with rasterio.open(
            tif, "w", driver="COG", dtype="int32", count=1,
            height=meta["shape"][0], width=meta["shape"][1],
            crs=meta["crs"], transform=meta["transform"], nodata=-1,
            compress="deflate", blocksize=512,
        ) as dst:
            dst.write(cell_index, 1)
        upload(tif, f"{GRID_PREFIX}/cell_index.tif")
        pq = Path(tmp) / "cells.parquet"
        df.to_parquet(pq, index=False)
        upload(pq, f"{GRID_PREFIX}/cells.parquet")
    print(f"wrote s3://{BUCKET}/{GRID_PREFIX}/cell_index.tif and cells.parquet")


if __name__ == "__main__":
    main()