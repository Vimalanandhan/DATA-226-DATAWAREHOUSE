-- models/marts/dim_address.sql
{{ config(materialized='table') }}

select distinct
  individual_id,
  city,
  state,
  county,
  lat,
  lon
from {{ ref('int_customer_enriched') }}
