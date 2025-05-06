

with cust as (
    select
        individual_id,
        annual_premium,
        days_tenure,
        cust_orig_date,
        age_in_years,
        date_of_birth,
        address_id
    from USER_DB_FALCON.RAW_public.stg_customer
),

demo as (
    select
        individual_id,
        has_children,
        length_of_residence,
        marital_status,
        home_owner,
        home_market_value,
        college_degree,
        good_credit
    from USER_DB_FALCON.RAW_public.stg_demographic
),

addr as (
    select
        address_id,
        city,
        state,
        county,
        lat,
        lon
    from USER_DB_FALCON.RAW_public.stg_address
)

select
    cust.individual_id,
    cust.annual_premium,
    cust.days_tenure,
    cust.cust_orig_date,
    cust.age_in_years,
    cust.date_of_birth,
    demo.has_children,
    demo.length_of_residence,
    demo.marital_status,
    demo.home_owner,
    demo.home_market_value,
    addr.city,
    addr.state,
    addr.county,
    addr.lat,
    addr.lon
from cust
left join demo using (individual_id)
left join addr using (address_id)