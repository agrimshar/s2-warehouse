-- One row per land cell per year: summer median NDVI (the question's metric)
-- and growing-season peak NDVI (the classifier), from observations that were
-- mostly covered and mostly clear.
with obs as (
    select
        f.cell_idx,
        d.year,
        d.in_summer_window,
        d.month between 5 and 9 as in_growing_season,
        f.ndvi_mean
    from {{ ref('fact_measurement') }} as f
    join {{ ref('dim_date') }} as d on d.date_day = f.observed_on
    join {{ ref('dim_region') }} as r using (cell_idx)
    where r.is_land
      and f.coverage_pct >= 70
      and f.clear_pct >= 70
)
select
    cell_idx,
    year,
    median(ndvi_mean) filter (where in_summer_window)                as ndvi_summer_median,
    count(*) filter (where in_summer_window)                         as n_summer_obs,
    quantile_cont(ndvi_mean, 0.9) filter (where in_growing_season)   as ndvi_season_p90,
    count(*) filter (where in_growing_season)                        as n_season_obs
from obs
group by 1, 2