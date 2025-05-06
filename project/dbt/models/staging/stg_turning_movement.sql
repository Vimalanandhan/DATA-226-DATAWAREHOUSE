-- models/staging/stg_turning_movement.sql
{{ config(materialized='view') }}
select
  objectid,
  facilityid,
  intid,
  intname,
  nodeno,
  longitude::float as tm_lon,
  latitude::float  as tm_lat
from {{ source('raw', 'TURNING_MOVEMENT') }}
