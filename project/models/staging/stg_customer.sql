{{ config(materialized='view') }}

select
  individual_id,
  address_id,
  curr_ann_amt    as annual_premium,
  days_tenure,
  cust_orig_date,
  age_in_years,            -- bring this in
  date_of_birth            -- and this
from {{ source('raw', 'CUSTOMER_STAGING') }}
