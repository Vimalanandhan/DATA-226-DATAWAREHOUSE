
  create or replace   view USER_DB_FALCON.RAW_public.stg_autoinsurance_churn
  
   as (
    

select
  individual_id,
  churn              as churn_flag
from USER_DB_FALCON.RAW.AUTOINSURANCE_CHURN_STAGING
  );

