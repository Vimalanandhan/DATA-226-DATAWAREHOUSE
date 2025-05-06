{{ config(materialized='view') }}

select
  individual_id,
  churn              as churn_flag
from {{ source('raw', 'AUTOINSURANCE_CHURN_STAGING') }}
