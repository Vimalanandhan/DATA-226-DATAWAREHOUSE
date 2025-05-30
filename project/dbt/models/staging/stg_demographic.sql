{{ config(materialized='view') }}

select
  individual_id,
  income,
  has_children,
  length_of_residence    as length_of_residence,  -- exact match
  marital_status,
  home_owner,
  home_market_value      as home_market_value,
  college_degree,
  good_credit
from {{ source('raw', 'DEMOGRAPHIC_STAGING') }}
