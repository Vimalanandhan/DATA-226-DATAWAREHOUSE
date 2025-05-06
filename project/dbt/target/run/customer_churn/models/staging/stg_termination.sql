
  create or replace   view USER_DB_FALCON.RAW_public.stg_termination
  
   as (
    

select
  individual_id,
  acct_suspd_date    as churn_date
from USER_DB_FALCON.RAW.TERMINATION_STAGING
  );

