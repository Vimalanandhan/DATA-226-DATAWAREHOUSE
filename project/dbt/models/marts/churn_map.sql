-- models/marts/churn_map.sql
{{ config(materialized='table') }}

select
  f.individual_id,
  f.churn_date,
  f.churn_flag,
  a.lon,
  a.lat
from {{ ref('fact_churn') }}      as f
join {{ ref('dim_address') }}    as a
  using (individual_id)
