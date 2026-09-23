-- One row per land cell with enough clear summer observations in both years.
-- delta_summer answers the question; label says what kind of change it looks like.
with c25 as (select * from {{ ref('cell_year_composite') }} where year = 2025),
c26 as (select * from {{ ref('cell_year_composite') }} where year = 2026),
cells as (
    select
        c25.cell_idx,
        c25.ndvi_summer_median                          as summer_2025,
        c26.ndvi_summer_median                          as summer_2026,
        c26.ndvi_summer_median - c25.ndvi_summer_median as delta_summer,
        c25.ndvi_season_p90                             as peak_2025,
        c26.ndvi_season_p90                             as peak_2026,
        c26.ndvi_season_p90 - c25.ndvi_season_p90       as delta_peak,
        c25.n_summer_obs                                as n_obs_2025,
        c26.n_summer_obs                                as n_obs_2026
    from c25
    join c26 using (cell_idx)
    where c25.n_summer_obs >= 4 and c26.n_summer_obs >= 4
),
nbr as (
    select
        n.cell_idx,
        median(x.delta_summer) as neighbour_delta_summer,
        count(*)               as n_neighbours
    from {{ ref('stg_neighbours') }} as n
    join cells as x on x.cell_idx = n.neighbour_idx
    group by 1
)
select
    c.*,
    r.h3_cell,
    r.lat,
    r.lon,
    nb.neighbour_delta_summer,
    nb.n_neighbours,
    c.delta_summer - nb.neighbour_delta_summer as local_excess,
    case
        when c.delta_peak <= -0.2 and c.peak_2026 < 0.35
             and c.delta_summer - nb.neighbour_delta_summer <= -0.1 then 'conversion'
        when c.delta_summer <= -0.1 and nb.neighbour_delta_summer <= -0.05 then 'regional'
        when c.delta_summer <= -0.1 and c.peak_2026 >= 0.5 then 'rotation_or_stress'
        when c.delta_summer <= -0.1 then 'local_unclassified'
        else 'stable'
    end as label,
    rank() over (order by c.delta_summer) as loss_rank
from cells as c
join {{ ref('dim_region') }} as r using (cell_idx)
left join nbr as nb using (cell_idx)