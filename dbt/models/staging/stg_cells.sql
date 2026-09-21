select
    cast(cell_idx as integer) as cell_idx,
    h3_cell,
    lat,
    lon,
    area_km2,
    pixels_in_tile,
    case when isnan(water_pct) then null else water_pct end as water_pct
from read_parquet('s3://{{ var("s3_bucket") }}/grid/tile=17TPJ/cells.parquet')