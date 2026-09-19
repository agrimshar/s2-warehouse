"""Bronze ingestion: land 4 bands per scene in s3 at 20m"""

import argparse
import tempfile
import time
from functools import partial
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from botocore.exceptions import BotoCoreError, ClientError
from rasterio import Affine

from s2warehouse.raster import GDAL_ENV
from s2warehouse.storage import BUCKET, bronze_key, download, object_exists, upload

BANDS_10M = ["B03", "B04", "B08"]
BANDS_20M = ["SCL"]
NODATA = 0
MANIFEST_KEY = "bronze/manifest/manifest.parquet"

def downsample_2x2(a: np.ndarray, nodata: int = NODATA) -> np.ndarray:
    """Mean of each 2x2 block. All nodata blocks stay as no data"""
    h2, w2 = a.shape[0] // 2, a.shape[1] // 2
    blocks = a[: h2 * 2, : w2 * 2].reshape(h2, 2, w2, 2)
    valid = blocks != nodata
    total = (blocks * valid).sum(axis=(1, 3), dtype="int64")
    count = valid.sum(axis=(1, 3))
    mean = np.zeros((h2, w2), dtype="float64")
    np.divide(total, count, out=mean, where=count>0)
    out = np.rint(mean).astype(a.dtype)
    out[count == 0] = nodata
    return out

def retry(fn, attempts: int = 4, base_delay: float = 2.0):
    """Call fn(). On I/O or AWS failure wait base_delay * 2**k and try again"""
    for k in range(attempts):
        try:
            return fn()
        except(OSError, BotoCoreError, ClientError) as e:
            if k == attempts - 1:
                raise
            delay = base_delay * 2**k
            print(f"    retry {k+1} after {type(e).__name__}, waiting {delay:.0f}s")
            time.sleep(delay)
    return None

def read_band_20m(href: str, downsample: bool) -> tuple[np.ndarray, dict]:
    with rasterio.Env(**GDAL_ENV), rasterio.open(href) as src:
        arr = src.read(1)
        crs, transform = src.crs, src.transform
    if downsample:
        arr = downsample_2x2(arr)
        transform = transform * Affine.scale(2)
    return arr, {"crs": crs, "transform": transform}

def write_cog(path: Path, arr: np.ndarray, crs, transform) -> None:
    with rasterio.open(
        path, "w", driver="COG", dtype=arr.dtype, count=1,
        height=arr.shape[0], width=arr.shape[1],
        crs=crs, transform=transform, nodata=NODATA,
        compress="deflate", predictor=2, blocksize=512,
    ) as dst:
        dst.write(arr, 1)

def ingest_scene(row: pd.Series, workdir: Path) -> dict:
    scene_id = row["scene_id"]
    written = skipped = 0
    for band in BANDS_10M + BANDS_20M:
        key = bronze_key(scene_id, band)
        if object_exists(key):
            skipped += 1
            continue
        arr, meta = retry(partial(read_band_20m, row[f"href_{band}"], band in BANDS_10M))
        local = workdir / f"{scene_id}_{band}.tif"
        write_cog(local, arr, meta["crs"], meta["transform"])
        retry(partial(upload, local, key))
        local.unlink()
        written += 1
    return {"scene_id": scene_id, "written": written, "skipped": skipped}

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=3)
    p.add_argument("--start", default="2025-01-01")
    p.add_argument("--end", default="2100-01-01")
    args = p.parse_args()
    start = pd.Timestamp(args.start, tz="UTC")
    end = pd.Timestamp(args.end, tz="UTC")

    t0 = time.perf_counter()
    results = []
    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        download(MANIFEST_KEY, workdir / "manifest.parquet")
        df = pd.read_parquet(workdir / "manifest.parquet")
        df = df[(df["datetime"] >= start) & (df["datetime"] < end)]
        df = df.sort_values("datetime").head(args.limit)
        print(f"{len(df)} scenes to check in s3://{BUCKET}")
        for _, row in df.iterrows():
            ts = time.perf_counter()
            r = ingest_scene(row, workdir)
            r["seconds"] = round(time.perf_counter() - ts, 1)
            print(f"  {r['scene_id']}  wrote={r['written']} skipped={r['skipped']}  {r['seconds']}s")
            results.append(r)

    s = pd.DataFrame(results)
    print(f"\nscenes={len(s)} bands_written={s['written'].sum()} "
          f"bands_skipped={s['skipped'].sum()} total={time.perf_counter() - t0:.0f}s")

if __name__ == "__main__":
    main()