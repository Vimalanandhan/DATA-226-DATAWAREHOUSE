-- models/marts/fact_churn.sql
{{ config(
    materialized='incremental',
    unique_key=['individual_id','churn_date']
) }}

select
  t.individual_id,
  t.churn_date,
  e.city,
  e.state,
  e.county,
  e.age_in_years,
  e.days_tenure,
  e.annual_premium,
  c.churn_flag
from {{ ref('stg_termination') }} t
join {{ ref('int_customer_enriched') }} e
  using (individual_id)
left join {{ ref('stg_autoinsurance_churn') }} c
  using (individual_id)

{% if is_incremental() %}
where t.churn_date > (select max(churn_date) from {{ this }})
{% endif %}

