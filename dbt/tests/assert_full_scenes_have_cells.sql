select
    s.scene_id,
    s.measured_coverage_pct,
    count(*) filter (where m.observed_count > 0) as observed_cells
from {{ ref('dim_scene') }} as s
join {{ ref('stg_measurements') }} as m using (scene_id)
where s.is_full_tile
group by 1, 2
having count(*) filter (where m.observed_count > 0) < 2311 * 0.9