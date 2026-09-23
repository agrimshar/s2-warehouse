select max(observed_on) as latest
from {{ ref('fact_measurement') }}
having max(observed_on) < current_date - interval 14 day