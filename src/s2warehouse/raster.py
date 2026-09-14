"""Read Sentinel-2 bands (Cloud-Optimised GeoTIFFs) over HTTPS"""

import time

import numpy as np
import pandas as pd
import rasterio
from rasterio.windows import Window

GDAL_ENV = {
    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif",
    "GDAL_HTTP_MULTIPLX": "YES",
    "VSI_CACHE": "TRUE",
}

SCL_CLASSES = {
    0: "no_data",
    1: "saturated",
    2: "dark",
    3: "cloud_shadow",
    4: "vegetation",
    5: "not_vegetated",
    6: "water",
    7: "unclassified",
    8: "cloud_medium",
    9: "cloud_high",
    10: "thin_cirrus",
    11: "snow"
}

def read_band(href: str, window: Window | None = None) -> tuple[np.ndarray, dict]:
    """Read on band, or one window if it. Returns (array, metadata)"""
    with rasterio.Env(**GDAL_ENV), rasterio.open(href) as src:
        arr = src.read(1, window=window)
        meta = {
            "crs": str(src.crs),
            "shape": src.shape,
            "dtype": src.dtypes[0],
            "nodata": src.nodata,
            "transform": src.transform,
            "overviews": src.overviews(1),
        }
    return arr, meta

def ndvi(red: np.ndarray, nir: np.ndarray) -> np.ndarray:
    """(NIR - red) / (NIR + red). NaN where the denominator is 0"""
    red = red.astype("float32")
    nir = nir.astype("float32")
    denom = nir + red
    out = np.full(red.shape, np.nan, dtype="float32")
    np.divide(nir - red, denom, out=out, where=denom > 0)
    return out

if __name__ == "__main__":
    df = pd.read_parquet("manifest.parquet")
    july = df[(df["datetime"].dt.year == 2025) & (df["datetime"].dt.month == 7)]
    full = july[july["nodata_pct"] < 5]
    scene = full.sort_values("cloud_cover").iloc[0]
    print(
        f"scene {scene['scene_id']}  cloud {scene['cloud_cover']:.1f}%  "
        f"nodata {scene['nodata_pct']:.1f}%"
    )

    # 1. SCL full band. 20 m, 5490x5490 uint8
    t0 = time.perf_counter()
    scl, meta = read_band(scene["href_SCL"])
    dt = time.perf_counter() - t0
    print(f"\nSCL full read: {dt:.1f}s shape={scl.shape} crs={meta['crs']}")
    print("overviews:", meta["overviews"])
    counts = np.bincount(scl.ravel(), minlength=12)
    for cls, n in enumerate(counts):
        name = SCL_CLASSES.get(cls, "?")
        print(f"  {cls:2d} {name:<14} {100 * n / scl.size:5.1f}%")

    # 2. A 512 x 512 window of red and NIR at 10 m
    win = Window(col_off=5000, row_off=5000, width=512, height=512)
    t0 = time.perf_counter()
    red, _ = read_band(scene["href_B04"], win)
    nir, _ = read_band(scene["href_B08"], win)
    dt = time.perf_counter() - t0
    print(f"\nred+nir window read: {dt:.1f}s shape={red.shape} dtype={red.dtype}")
    v = ndvi(red, nir)
    valid = np.isfinite(v)
    print(f"valid pixels in window: {100 * valid.mean():.1f}%")
    if valid.any():
        print(f"NDVI window: mean={np.nanmean(v):.3f} "
              f"min={np.nanmin(v):.3f} max={np.nanmax(v):.3f}")

    # 3. Red, full band. 10 m, so 10980 x 10980 uint16
    t0 = time.perf_counter()
    red_full, _ = read_band(scene["href_B04"])
    dt = time.perf_counter() - t0
    print(f"\nred full read: {dt:.1f}s shape={red_full.shape}")