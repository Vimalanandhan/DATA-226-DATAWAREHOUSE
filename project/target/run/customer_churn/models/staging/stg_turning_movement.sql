
  create or replace   view USER_DB_FALCON.RAW_public.stg_turning_movement
  
   as (
    -- models/staging/stg_turning_movement.sql

select
  objectid,
  facilityid,
  intid,
  intname,
  nodeno,
  longitude::float as tm_lon,
  latitude::float  as tm_lat
from USER_DB_FALCON.RAW.TURNING_MOVEMENT
  );

