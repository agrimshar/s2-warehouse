select
    cell_idx,
    h3_cell,
    lat,
    lon,
    area_km2,
    pixels_in_tile,
    water_pct,
    pixels_in_tile > 0                                    as in_tile,
    pixels_in_tile > 0 and coalesce(water_pct, 100) < 90  as is_land
from {{ ref('stg_cells') }}