select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select churn_date
from USER_DB_FALCON.RAW_public.fact_churn
where churn_date is null



      
    ) dbt_internal_test