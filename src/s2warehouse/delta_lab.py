"""Delta mechanics on the silver table: OPTIMIZE, Z-ORDER, time travel, schema, VACUUM."""

import argparse
import shutil
import time
from pathlib import Path

from delta import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import Row
from pyspark.sql import functions as F

from s2warehouse.silver import SILVER, build_spark


def detail(spark, path: str) -> tuple[int, int]:
    d = DeltaTable.forPath(spark, path).detail().select("numFiles", "sizeInBytes").first()
    return d["numFiles"], d["sizeInBytes"]


def cell_query(spark, path: str, cell: int) -> tuple[int, float]:
    t0 = time.perf_counter()
    n = spark.read.format("delta").load(path).where(F.col("cell_idx") == cell).count()
    return n, time.perf_counter() - t0


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--path", default=str(SILVER))
    p.add_argument("--cell", type=int, default=1000)
    p.add_argument("--max-file-bytes", type=int, default=200_000)
    p.add_argument("--enable-dv", action="store_true")
    args = p.parse_args()
    spark = build_spark()
    abs_path = str(Path(args.path).resolve())
    tbl = DeltaTable.forPath(spark, args.path)

    if args.enable_dv:
        spark.sql(
            f"ALTER TABLE delta.`{abs_path}` "
            "SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')"
        )
        print("deletion vectors enabled")
        spark.stop()
        return

    files, size = detail(spark, args.path)
    n, dt = cell_query(spark, args.path, args.cell)
    print(f"before: {files} files, {size:,} B; cell {args.cell}: {n} rows in {dt:.2f}s")

    spark.conf.set("spark.databricks.delta.optimize.maxFileSize", args.max_file_bytes)
    spark.sparkContext.setJobDescription("lab: OPTIMIZE ZORDER BY cell_idx")
    t0 = time.perf_counter()
    tbl.optimize().executeZOrderBy("cell_idx")
    print(f"optimize + zorder: {time.perf_counter() - t0:.1f}s")

    files, size = detail(spark, args.path)
    n, dt = cell_query(spark, args.path, args.cell)
    print(f"after:  {files} files, {size:,} B; cell {args.cell}: {n} rows in {dt:.2f}s")

    v0 = spark.read.format("delta").option("versionAsOf", 0).load(args.path)
    print(f"time travel: version 0 has {v0.count()} rows, "
          f"{v0.select('scene_id').distinct().count()} scenes")
    tbl.history().select("version", "operation").show()

    lab = "data/delta/_lab"
    shutil.rmtree(lab, ignore_errors=True)
    spark.createDataFrame([Row(k=1, v=0.5)]).write.format("delta").save(lab)
    try:
        spark.createDataFrame([Row(k=2, v="oops")]).write.format("delta").mode("append").save(lab)
        print("schema enforcement: wrong type ACCEPTED (unexpected)")
    except AnalysisException as e:
        print(f"schema enforcement: rejected -> {str(e).splitlines()[0][:110]}")
    (
        spark.createDataFrame([Row(k=3, v=0.7, note="new")])
        .write.format("delta").mode("append").option("mergeSchema", "true").save(lab)
    )
    print("schema evolution: columns now", spark.read.format("delta").load(lab).columns)
    shutil.rmtree(lab)

    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    dead = spark.sql(f"VACUUM delta.`{abs_path}` RETAIN 0 HOURS DRY RUN")
    print(f"vacuum dry run: {dead.count()} files would be deleted at 0 h retention")
    spark.stop()


if __name__ == "__main__":
    main()