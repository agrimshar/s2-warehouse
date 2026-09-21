with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2025-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}
),
days as (
    select cast(date_day as date) as date_day from spine
)
select
    date_day,
    extract(year from date_day)          as year,
    extract(month from date_day)         as month,
    extract(day from date_day)           as day,
    dayofyear(date_day)                  as day_of_year,
    strftime(date_day, '%Y-%m')          as year_month,
    case
        when extract(month from date_day) in (12, 1, 2) then 'winter'
        when extract(month from date_day) in (3, 4, 5)  then 'spring'
        when extract(month from date_day) in (6, 7, 8)  then 'summer'
        else 'autumn'
    end                                  as season,
    (extract(month from date_day) = 6 and extract(day from date_day) >= 15)
        or extract(month from date_day) in (7, 8)
                                         as in_summer_window
from days