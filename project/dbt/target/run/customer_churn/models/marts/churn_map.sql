
  
    

        create or replace transient table USER_DB_FALCON.RAW_public.churn_map
         as
        (-- models/marts/churn_map.sql


select
  f.individual_id,
  f.churn_date,
  f.churn_flag,
  a.lon,
  a.lat
from USER_DB_FALCON.RAW_public.fact_churn      as f
join USER_DB_FALCON.RAW_public.dim_address    as a
  using (individual_id)
        );
      
  