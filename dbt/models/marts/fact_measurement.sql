{{ config(
    materialized='incremental',
    unique_key=['scene_id', 'cell_idx'],
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

select
    m.scene_id,
    m.cell_idx,
    m.observed_on,
    m.ndvi_mean,
    m.ndvi_std,
    m.ndwi_mean,
    m.pixel_count,
    m.valid_count,
    m.coverage_pct,
    m.observed_count,
    m.clear_pct
from {{ ref('stg_measurements') }} as m
where m.valid_count > 0
{% if is_incremental() %}
  -- 30-day lookback: new scenes and recently reprocessed ones.
  -- Older reprocessing needs --full-refresh. See DECISIONS.md.
  and m.observed_on >= (select max(observed_on) from {{ this }}) - interval 30 day
{% endif %}