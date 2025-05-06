
  
    

        create or replace transient table USER_DB_FALCON.RAW_public.dim_customer
         as
        (-- models/marts/dim_customer.sql


select distinct
  individual_id,
  age_in_years as age,
  marital_status,
  has_children,
  length_of_residence
from USER_DB_FALCON.RAW_public.int_customer_enriched
        );
      
  