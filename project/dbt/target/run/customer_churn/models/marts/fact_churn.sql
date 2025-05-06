-- back compat for old kwarg name
  
  begin;
    
        
            
                
                
            
                
                
            
        
    

    

    merge into USER_DB_FALCON.RAW_public.fact_churn as DBT_INTERNAL_DEST
        using USER_DB_FALCON.RAW_public.fact_churn__dbt_tmp as DBT_INTERNAL_SOURCE
        on (
                    DBT_INTERNAL_SOURCE.individual_id = DBT_INTERNAL_DEST.individual_id
                ) and (
                    DBT_INTERNAL_SOURCE.churn_date = DBT_INTERNAL_DEST.churn_date
                )

    
    when matched then update set
        "INDIVIDUAL_ID" = DBT_INTERNAL_SOURCE."INDIVIDUAL_ID","CHURN_DATE" = DBT_INTERNAL_SOURCE."CHURN_DATE","CITY" = DBT_INTERNAL_SOURCE."CITY","STATE" = DBT_INTERNAL_SOURCE."STATE","COUNTY" = DBT_INTERNAL_SOURCE."COUNTY","AGE_IN_YEARS" = DBT_INTERNAL_SOURCE."AGE_IN_YEARS","DAYS_TENURE" = DBT_INTERNAL_SOURCE."DAYS_TENURE","ANNUAL_PREMIUM" = DBT_INTERNAL_SOURCE."ANNUAL_PREMIUM","CHURN_FLAG" = DBT_INTERNAL_SOURCE."CHURN_FLAG"
    

    when not matched then insert
        ("INDIVIDUAL_ID", "CHURN_DATE", "CITY", "STATE", "COUNTY", "AGE_IN_YEARS", "DAYS_TENURE", "ANNUAL_PREMIUM", "CHURN_FLAG")
    values
        ("INDIVIDUAL_ID", "CHURN_DATE", "CITY", "STATE", "COUNTY", "AGE_IN_YEARS", "DAYS_TENURE", "ANNUAL_PREMIUM", "CHURN_FLAG")

;
    commit;