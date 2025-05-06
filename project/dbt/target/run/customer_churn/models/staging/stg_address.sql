
  create or replace   view USER_DB_FALCON.RAW_public.stg_address
  
   as (
    -- models/staging/stg_address.sql

select
  address_id,
  street_address,
  city,
  state,
  county,
  latitude::float as lat,
  longitude::float as lon
from USER_DB_FALCON.RAW.ADDRESS_STAGING
  );

