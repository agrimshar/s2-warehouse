{{ config(severity='warn') }}

select scene_id, observed_on, measured_clear_pct
from {{ ref('dim_scene') }}
where observed_on = (select max(observed_on) from {{ ref('dim_scene') }})
  and measured_clear_pct < 10