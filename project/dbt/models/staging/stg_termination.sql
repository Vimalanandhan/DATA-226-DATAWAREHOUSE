{{ config(materialized='view') }}

select
  individual_id,
  acct_suspd_date    as churn_date
from {{ source('raw', 'TERMINATION_STAGING') }}
