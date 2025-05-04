-- models/staging/stg_address.sql
{{ config(materialized='view') }}
select
  address_id,
  street_address,
  city,
  state,
  county,
  latitude::float as lat,
  longitude::float as lon
from {{ source('raw', 'ADDRESS_STAGING') }}
