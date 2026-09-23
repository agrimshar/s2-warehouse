# Proofs

Each entry is a measured result, reproducible from the repo.

## Idempotent bronze ingestion, 2026-09-20
Backfill of all 157 scenes from a laptop: 612 bands written, 16 skipped
(the 4 scenes landed earlier), 10,050 s, zero failures. Rerun of any scene
writes 0 objects. Same result from the Docker image: 1 scene checked,
wrote=0 skipped=4, proving the container sees the same S3 state.

## Skew, caused and observed, 2026-09-19
Same 157 scenes, same per-scene function, same 4 cores, 24 min of total task
time in both runs.
One task per scene (157 tasks, 1 stage): duration min/median/max = 2 s / 9 s /
38 s, stage 6.1 min, core utilisation 98 %.
Partition by month (24 tasks, 2 stages): duration min/median/max = 3 ms /
1.1 min / 5.0 min, job 7.6 to 9 min. At least 6 partitions empty. Largest
partition 27 scenes (62,397 rows), 5.0 min on one core. Last task launched
5.6 min in and ran 3.3 min with other cores idle.
Cause: a low-cardinality, uneven key hashed into 24 slots. Fix here: partition
on the unit of work. Fix when the key is required: salt it.

## Idempotent MERGE into Delta silver, 2026-09-19
Full run: rows=362827, scenes=157. Re-run of 5 scenes: rows=362827, MERGE
updated 11555, inserted 0, removed 4 files, added 2, only in the partitions
those scenes belong to.

## Delta data skipping and copy-on-write vs merge-on-read, 2026-09-19
Before OPTIMIZE: 81 live files, 5.59 MB; a cell query must read 81 of 81.
After OPTIMIZE ZORDER BY cell_idx: 23 live files, one per month partition,
4.59 MB; reads 21 of 23. Compaction dominated; Z-order skipping only acts
inside partitions with more than one file. Time travel to v0: 362827 rows,
157 scenes. Dead files on disk pending VACUUM: 85.
MERGE 5 scenes, copy-on-write: 2 files removed, 2311 untouched rows copied.
MERGE 5 scenes, deletion vectors: 1 file removed (100 % matched), 1 DV added,
0 rows copied.

## dbt incremental, both halves, 2026-09-21
Full build: fact_measurement = 259877 rows. Deleted Aug to Sep 2026 (234004
left), incremental run restored 259877. Deleted Jul 2025 (243225 left),
incremental run did NOT restore it, outside the 30-day lookback by design;
--full-refresh restored 259877.

## Data test caught wrong source metadata, 2026-09-21
assert_full_scenes_have_cells failed on S2B_17TPJ_20251201_0_L2A: STAC
nodata_pixel_percentage 0.00002, but 74.7 % of pixels are zero in all three
reflectance bands (diagonal swath edge, rows 4071 to 5487 fully empty) and
only 836 of 2311 cells have valid data. Bronze matches source. Resolution:
coverage is measured from pixels in dim_scene; the metadata value is kept
and a warn-severity test flags disagreement above 5 points.

## Cloud mask before/after, 2026-09-22
Silver rebuilt with the SCL mask (overwrite, v5 -> v7). fact_measurement rows
259877 -> 212832: 18 % of cell observations were fully cloudy and are now
absent rather than wrong. dbt: 31 tests, 30 pass, 1 warn (declared vs
measured nodata, S2B_17TPJ_20251201_0_L2A). Cell __: __ summer readings below
0.3 NDVI before masking, __ after. Chart: docs/cloud_mask_before_after.png.

## Cloud mask before/after, 2026-09-22
Silver rebuilt with the SCL mask (overwrite, v5 -> v7). fact_measurement rows
259877 -> 212832: 18 % of cell observations were fully cloudy and are now
absent rather than wrong. dbt: 31 tests, 30 pass, 1 warn (declared vs
measured nodata, S2B_17TPJ_20251201_0_L2A). Cell 293 (highest July NDVI
land cell), 2025: 91 observations before, 74 after. The 17 removed dates had
median NDVI 0.15; the 86 kept dates shifted +0.02. Residual dips on 1 Aug
and mid Sep show SCL missing haze; the composite uses a median across dates
for that reason. Chart: docs/cloud_mask_before_after.png.

## Orchestration: rerun and replay, 2026-09-23
Airflow 3.3.2, LocalExecutor, s2_daily on CronDataIntervalTimetable
("0 12 * * *"), catchup from 2026-09-01: 21 runs, 15 ended at the gate (no
scene), 6 ran the full chain (scenes on 4, 7, 11, 12, 14, 17 Sep). The 17 Sep
scene was not in the warehouse before: found by the catalog task, ingested,
merged (Inserted 2311, Updated 0) and modelled with no manual action;
fact_measurement 212832 -> 214133, max date 2026-09-17.
Cleared and replayed all 21 runs: 10 min 39 s wall, every run re-executed
(new attempt logs, start times after the clear), every re-merge Updated 2311
/ Inserted 0, fact_measurement 214133 unchanged. Runs with no scene take 15
to 20 s, runs with a scene 55 to 65 s. Every task is parameterised by
data_interval_start/end; nothing reads now().