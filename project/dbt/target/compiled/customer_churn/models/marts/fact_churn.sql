-- models/marts/fact_churn.sql


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
from USER_DB_FALCON.RAW_public.stg_termination t
join USER_DB_FALCON.RAW_public.int_customer_enriched e
  using (individual_id)
left join USER_DB_FALCON.RAW_public.stg_autoinsurance_churn c
  using (individual_id)


where t.churn_date > (select max(churn_date) from USER_DB_FALCON.RAW_public.fact_churn)
