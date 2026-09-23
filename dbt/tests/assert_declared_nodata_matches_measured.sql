{{ config(severity='warn') }}

select
    scene_id,
    declared_nodata_pct,
    round(100 - measured_coverage_pct, 2) as measured_nodata_pct
from {{ ref('dim_scene') }}
where abs((100 - measured_coverage_pct) - declared_nodata_pct) > 5