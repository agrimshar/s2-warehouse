"""Silver: per-cell, per-scene NDVI and NDWI. One Spark task per scene."""

import argparse
import os
import sys
import tempfile
import time
from datetime import date
from pathlib import Path

import boto3
import numpy as np
import pandas as pd
import rasterio
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from rasterio.session import AWSSession

from s2warehouse.grid import GRID_PREFIX
from s2warehouse.ingest import MANIFEST_KEY
from s2warehouse.paths import SILVER, STAGING
from s2warehouse.raster import GDAL_ENV, SCL_KEEP, ndvi, ndwi
from s2warehouse.storage import BUCKET, bronze_key, download, list_complete_scene_ids

SCHEMA = T.StructType([
    T.StructField("scene_id", T.StringType(), nullable=False),
    T.StructField("date", T.DateType(), nullable=False),
    T.StructField("cell_idx", T.IntegerType(), nullable=False),
    T.StructField("ndvi_mean", T.FloatType(), nullable=True),
    T.StructField("ndvi_std", T.FloatType(), nullable=True),
    T.StructField("ndwi_mean", T.FloatType(), nullable=True),
    T.StructField("pixel_count", T.IntegerType(), nullable=False),
    T.StructField("observed_count", T.IntegerType(), nullable=False),
    T.StructField("valid_count", T.IntegerType(), nullable=False),
])

def read_s3_band(key: str) -> np.ndarray:
    env = rasterio.Env(session=AWSSession(boto3.Session()), **GDAL_ENV)
    with env, rasterio.open(f"s3://{BUCKET}/{key}") as src:
        return src.read(1)

def zonal_mean_std(values, cell_index, valid, n):
    """Per-cell mean and std over valid pixels, plus the valid count per cell."""
    v = values[valid]
    i = cell_index[valid]
    cnt = np.bincount(i, minlength=n)
    s1 = np.bincount(i, weights=v, minlength=n)
    s2 = np.bincount(i, weights=v * v, minlength=n)
    mean = np.full(n, np.nan)
    np.divide(s1, cnt, out=mean, where=cnt > 0)
    ex2 = np.full(n, np.nan)
    np.divide(s2, cnt, out=ex2, where=cnt > 0)
    std = np.sqrt(np.maximum(ex2 - mean * mean, 0))
    return mean, std, cnt

def _f(x: float) -> float | None:
    return None if np.isnan(x) else float(x)

def pixel_masks(red, nir, green, scl, assigned):
    """observed: the scene imaged the pixel. valid: observed and SCL says clear."""
    observed = assigned & (red > 0) & (nir > 0) & (green > 0) & (scl != 0)
    valid = observed & np.isin(scl, SCL_KEEP)
    return observed, valid

def process_scene(scene_id: str, day: date, n_cells: int) -> list[tuple]:
    cell_index = read_s3_band(f"{GRID_PREFIX}/cell_index.tif")
    red = read_s3_band(bronze_key(scene_id, "B04"))
    nir = read_s3_band(bronze_key(scene_id, "B08"))
    green = read_s3_band(bronze_key(scene_id, "B03"))
    scl = read_s3_band(bronze_key(scene_id, "SCL"))

    assigned = cell_index >= 0
    observed, valid = pixel_masks(red, nir, green, scl, assigned)
    ndvi_mean, ndvi_std, valid_count = zonal_mean_std(ndvi(red, nir), cell_index, valid, n_cells)
    ndwi_mean, _, _ = zonal_mean_std(ndwi(green, nir), cell_index, valid, n_cells)
    pixel_count = np.bincount(cell_index[assigned], minlength=n_cells)
    observed_count = np.bincount(cell_index[observed], minlength=n_cells)

    return [
        (scene_id, day, int(c), _f(ndvi_mean[c]), _f(ndvi_std[c]), _f(ndwi_mean[c]),
         int(pixel_count[c]), int(observed_count[c]), int(valid_count[c]))
        for c in np.flatnonzero(pixel_count > 0)
    ]

def build_spark() -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName("s2-silver")
        .master("local[4]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def upsert(spark: SparkSession, df, path: str, replace: bool = False) -> None:
    if replace or not DeltaTable.isDeltaTable(spark, path):
        (
            df.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true")
            .partitionBy("year_month").save(path)
        )
        return
    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(
            df.alias("s"),
            "t.year_month = s.year_month AND t.scene_id = s.scene_id "
            "AND t.cell_idx = s.cell_idx",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--out", default=str(SILVER))
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--skew", action="store_true", help="partition by month instead of scene")
    p.add_argument("--replace", action="store_true", help="overwrite the table")
    p.add_argument("--from-staging", action="store_true", help="skip compute, merge staging")
    p.add_argument("--wait", action="store_true", help="keep the Spark UI up until Enter")
    args = p.parse_args()

    spark = build_spark()
    t0 = time.perf_counter()

    if not args.from_staging:
        with tempfile.TemporaryDirectory() as tmp:
            download(MANIFEST_KEY, Path(tmp) / "manifest.parquet")
            manifest = pd.read_parquet(Path(tmp) / "manifest.parquet")
            download(f"{GRID_PREFIX}/cells.parquet", Path(tmp) / "cells.parquet")
            n_cells = len(pd.read_parquet(Path(tmp) / "cells.parquet"))

        ready = set(list_complete_scene_ids())
        m = manifest[manifest.scene_id.isin(ready)].sort_values("datetime")
        if args.limit:
            m = m.head(args.limit)
        scenes = list(zip(m.scene_id, m.datetime.dt.date))
        print(f"{len(scenes)} complete scenes in bronze, {n_cells} cells")

        if args.skew:
            keyed = spark.sparkContext.parallelize(scenes).keyBy(lambda s: s[1].strftime("%Y-%m"))
            rdd = keyed.partitionBy(24).values()
        else:
            rdd = spark.sparkContext.parallelize(scenes, numSlices=len(scenes))
        rows = rdd.flatMap(lambda s: process_scene(s[0], s[1], n_cells))
        df = (
            spark.createDataFrame(rows, SCHEMA)
            .withColumn("coverage_pct", F.round(100 * F.col("observed_count") / F.col("pixel_count"), 2))
            .withColumn(
                "clear_pct",
                F.round(100 * F.col("valid_count") / F.nullif(F.col("observed_count"), F.lit(0)), 2),
            )
            .withColumn("year_month", F.date_format("date", "yyyy-MM"))
        )
        spark.sparkContext.setJobDescription("silver: compute to staging")
        df.write.mode("overwrite").parquet(str(STAGING))
    t_compute = time.perf_counter() - t0

    spark.sparkContext.setJobDescription("silver: merge staging into delta")
    t1 = time.perf_counter()
    upsert(spark, spark.read.parquet(str(STAGING)), args.out, replace=args.replace)
    t_merge = time.perf_counter() - t1

    table = DeltaTable.forPath(spark, args.out).toDF()
    print(f"compute {t_compute:.0f}s  merge {t_merge:.0f}s")
    print(f"rows={table.count()}  scenes={table.select('scene_id').distinct().count()}")
    DeltaTable.forPath(spark, args.out).history(3).select(
        "version", "operation", "operationMetrics"
    ).show(truncate=False)

    if args.wait:
        input("Spark UI at http://localhost:4040 - press Enter to stop")
    spark.stop()
    


if __name__ == "__main__":
    main()