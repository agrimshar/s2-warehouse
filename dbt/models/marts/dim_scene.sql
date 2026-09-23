with measured as (
    select
        scene_id,
        100.0 * sum(observed_count) / sum(pixel_count)             as measured_coverage_pct,
        100.0 * sum(valid_count) / nullif(sum(observed_count), 0)  as measured_clear_pct
    from {{ ref('stg_measurements') }}
    group by 1
)
select
    s.scene_id,
    s.captured_at,
    s.observed_on,
    s.tile,
    s.satellite,
    s.cloud_cover                     as declared_cloud_pct,
    s.nodata_pct                      as declared_nodata_pct,
    m.measured_coverage_pct,
    m.measured_clear_pct,
    m.measured_coverage_pct >= 95     as is_full_tile
from {{ ref('stg_scenes') }} as s
left join measured as m using (scene_id)