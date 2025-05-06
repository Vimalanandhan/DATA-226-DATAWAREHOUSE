select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select individual_id
from USER_DB_FALCON.RAW_public.stg_customer
where individual_id is null



      
    ) dbt_internal_test