select s.scene_id, count(f.cell_idx) as cells
from {{ ref('dim_scene') }} as s
left join {{ ref('fact_measurement') }} as f using (scene_id)
where s.is_full_tile
group by 1
having count(f.cell_idx) < 2200