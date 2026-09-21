select
    cast(scene_id as varchar)  as scene_id,
    cast(date as date)         as observed_on,
    cast(cell_idx as integer)  as cell_idx,
    ndvi_mean,
    ndvi_std,
    ndwi_mean,
    pixel_count,
    valid_count,
    coverage_pct,
    year_month
from delta_scan('{{ var("silver_path") }}')