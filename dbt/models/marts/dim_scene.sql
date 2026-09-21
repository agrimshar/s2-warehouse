select
    scene_id,
    captured_at,
    observed_on,
    tile,
    satellite,
    cloud_cover,
    nodata_pct,
    nodata_pct < 5 as is_full_tile
from {{ ref('stg_scenes') }}