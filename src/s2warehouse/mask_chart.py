"""One cell's 2025 NDVI series: nodata mask only (old version) vs SCL cloud mask (current)."""

import argparse

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from delta import DeltaTable
from pyspark.sql import functions as F

from s2warehouse.paths import DOCS, SILVER, WAREHOUSE
from s2warehouse.silver import build_spark


def pick_forest_cell() -> int:
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    q = """
        select f.cell_idx
        from fact_measurement as f
        join dim_region as r using (cell_idx)
        where r.is_land
          and f.observed_on between date '2025-07-01' and date '2025-07-31'
        group by 1
        order by avg(f.ndvi_mean) desc
        limit 1
    """
    cell = con.execute(q).fetchone()[0]
    con.close()
    return int(cell)


def series(spark, path: str, cell: int, version: int | None) -> pd.DataFrame:
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    df = (
        reader.load(path)
        .where((F.col("cell_idx") == cell) & (F.year("date") == 2025) & (F.col("valid_count") > 0))
        .select("date", "ndvi_mean")
        .orderBy("date")
        .toPandas()
    )
    df["date"] = pd.to_datetime(df["date"])
    return df


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--path", default=str(SILVER))
    p.add_argument("--cell", type=int, default=None)
    p.add_argument("--before-version", type=int, default=None)
    p.add_argument("--out", default=str(DOCS / "cloud_mask_before_after.png"))
    args = p.parse_args()

    cell = args.cell if args.cell is not None else pick_forest_cell()
    spark = build_spark()
    current = DeltaTable.forPath(spark, args.path).history(1).first()["version"]
    before_v = args.before_version if args.before_version is not None else current - 1
    before = series(spark, args.path, cell, before_v)
    after = series(spark, args.path, cell, None)
    spark.stop()

    plt.switch_backend("Agg")
    fig, ax = plt.subplots(figsize=(11, 4.5))
    ax.plot(before["date"], before["ndvi_mean"], "o-", color="#999999",
            label=f"nodata mask only (silver v{before_v})")
    ax.plot(after["date"], after["ndvi_mean"], "o-", color="#2a7f3f",
            label=f"SCL cloud mask (silver v{current})")
    ax.set_ylim(-0.2, 1.0)
    ax.set_ylabel("mean NDVI")
    ax.set_title(f"Cell {cell}, 2025: the same pixels before and after cloud masking")
    ax.grid(alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(args.out, dpi=150)

    summer = (before["date"] >= "2025-06-01") & (before["date"] <= "2025-08-31")
    summer_after = (after["date"] >= "2025-06-01") & (after["date"] <= "2025-08-31")
    print(f"cell {cell}: {len(before)} observations before, {len(after)} after")
    print(f"summer readings below 0.3 NDVI: {(before.loc[summer, 'ndvi_mean'] < 0.3).sum()} before, "
          f"{(after.loc[summer_after, 'ndvi_mean'] < 0.3).sum()} after")
    print(f"wrote {args.out}")

    merged = before.merge(after, on="date", how="left", suffixes=("_before", "_after"))
    removed = merged[merged["ndvi_mean_after"].isna()]
    kept = merged.dropna()
    print(f"removed by mask: {len(removed)} dates, "
          f"median NDVI before = {removed['ndvi_mean_before'].median():.2f}")
    print(f"kept: {len(kept)} dates, median change after - before = "
          f"{(kept['ndvi_mean_after'] - kept['ndvi_mean_before']).median():+.3f}")


if __name__ == "__main__":
    main()