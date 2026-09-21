select
    scene_id,
    cast(datetime as timestamp) as captured_at,
    cast(datetime as date)      as observed_on,
    tile,
    cloud_cover,
    nodata_pct,
    substr(scene_id, 1, 3)      as satellite
from read_parquet('s3://{{ var("s3_bucket") }}/bronze/manifest/manifest.parquet')