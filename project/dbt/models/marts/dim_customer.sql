-- models/marts/dim_customer.sql
{{ config(materialized='table') }}

select distinct
  individual_id,
  age_in_years as age,
  marital_status,
  has_children,
  length_of_residence
from {{ ref('int_customer_enriched') }}
