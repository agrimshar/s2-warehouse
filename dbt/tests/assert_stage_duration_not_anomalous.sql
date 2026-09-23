{{ config(severity='warn') }}

with med as (
    select stage, median(seconds) as med_seconds
    from {{ ref('ops_stage_runs') }}
    where status = 'ok'
    group by 1
)
select r.ts, r.run_id, r.stage, r.seconds, m.med_seconds
from {{ ref('ops_stage_runs') }} as r
join med as m using (stage)
where r.status = 'ok'
  and r.ts >= now() - interval 7 day
  and r.seconds > 3 * m.med_seconds
  and r.seconds > 60