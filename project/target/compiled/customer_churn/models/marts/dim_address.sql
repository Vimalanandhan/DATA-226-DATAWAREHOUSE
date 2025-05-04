-- models/marts/dim_address.sql


select distinct
  individual_id,
  city,
  state,
  county,
  lat,
  lon
from USER_DB_FALCON.RAW_public.int_customer_enriched