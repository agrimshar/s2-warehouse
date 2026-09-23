{{ config(severity='warn') }}

select ts, run_id, stage
from {{ ref('ops_stage_runs') }}
where status = 'error'
  and ts >= now() - interval 7 day
