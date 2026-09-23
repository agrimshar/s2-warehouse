# Decisions

One entry per choice: what was decided, why, and what was rejected.

## 2026-09-14 Tile 17TPJ, H3 resolution 7
Locked the tile with the most scenes (156 vs 68 for 17TNJ; it sits in a
two-orbit overlap). H3 res 7 (~5.5 km2 here) gives ~2,300 cells per tile,
enough spatial detail to locate change, few enough rows to keep every layer
small. Rejected: two tiles (doubles everything for no new engineering).

## 2026-09-14 Coverage is a first-class column
About half the scenes are partial strips with 40 to 60 % nodata, and
eo:cloud_cover is computed over valid pixels only, so a 0 % cloud scene can
be 61 % empty. The manifest carries nodata_pct and every per-cell aggregate
carries coverage. Rejected: filtering partial scenes out; they still hold
good pixels for the cells they cover.

## 2026-09-15 Bronze at 20 m with a nodata-aware mean
10 m bands are downsampled 2x2 to 20 m so all four bands share SCL's grid
and bronze is ~20 GB instead of ~80. The mean ignores nodata pixels;
otherwise every strip edge would get a fake low reflectance. Rejected: the
COG's built-in overview, whose resampling method is undocumented.

## 2026-09-15 Bronze objects are the idempotency unit
Key = bronze/scene_id=<id>/<band>.tif. HEAD before every write; rerun writes
nothing. SCL is written last, so its presence marks a complete scene.

## 2026-09-19 Grid built by rasterising cell polygons
One int32 raster maps every 20 m pixel to a cell index, computed once and
shared by all scenes: ~2,600 hexagons rasterised in UTM, not 30 M per-pixel
H3 lookups. Cells with no pixels stay in the table so indices are stable.
Cells over 90 % water are excluded from the loss ranking.

## 2026-09-19 Silver: one Spark task per scene, RDD flatMap, no broadcast
Scenes are the unit of parallelism (parallelize with one slice per scene).
RDD flatMap because the input is a list and each element yields ~2,300
rows. The cell-index raster is read per task (2.3 MB COG) rather than
broadcast (120 MB pickled to every Python worker). Mean and std only; the
composite takes its median across dates in SQL.

## 2026-09-20 Skew experiment kept as a flag
--skew partitions scenes by month to reproduce a straggler on demand.

## 2026-09-20 Databricks dropped for the build
Free and trial workspaces cannot read a user-owned S3 bucket on serverless
compute. Open-source delta-spark locally gives identical Delta mechanics at
no cost. dbt therefore runs on DuckDB (dbt-duckdb) reading silver in place.

## 2026-09-20 Silver in Delta: month partitions, Z-ordered by cell, staged first
Partition for the write (one scene per merge touches one month), cluster for
the read (one cell's series crosses every month). Staging to Parquet before
MERGE is kept for a debuggable intermediate and a compute/commit boundary;
Delta already materialises MERGE sources, so it is not needed for
correctness. At this size the table is one file per month after OPTIMIZE,
so Z-order has nothing to skip; at scale the answer is liquid clustering.

## 2026-09-20 Deletion vectors on silver
Copy-on-write MERGE copied untouched rows sharing a file with touched ones.
DVs make the recurring one-scene merge cheap and push cost to OPTIMIZE.
DuckDB reads them fine.

## 2026-09-21 Star schema and incremental contract
Grain: one row per scene per cell with valid_count > 0; silver keeps the
footprint rows, gold drops them. Natural keys throughout. Business rules live
in dimensions: is_land, is_full_tile, and the 15 Jun to 31 Aug window as
dim_date.in_summer_window. fact_measurement is incremental, delete+insert on
(scene_id, cell_idx), 30-day lookback; older reprocessing needs
--full-refresh.

## 2026-09-22 Quality layer: SCL mask in Spark, coverage measured in dbt
Keep SCL 4,5,6,7 (incl. unclassified), drop 0,1,2,3,8,9,10,11 (incl. cast
shadows, snow). observed_count and valid_count are separate: coverage_pct is
observed/pixel (cloud-immune footprint), clear_pct is valid/observed.
dim_scene.is_full_tile derives from measured coverage; the STAC nodata figure
is kept as declared_nodata_pct and a warn-severity test flags disagreement
above 5 points (one scene, 2025-12-01). Full reprocessing is an overwrite
with overwriteSchema, keeping the pre-mask version for comparison.