{{ config(materialized='table') }}

with base as (
  select
    churn_date,
    churn_flag
  from {{ ref('fact_churn') }}
),

monthly as (
  select
    date_trunc('month', churn_date) as period,
    sum(churn_flag)                 as churn_count
  from base
  group by 1
),

by_state as (
  select
    state   as period,
    sum(churn_flag) as churn_count
  from {{ ref('fact_churn') }}
  group by 1
),

by_age as (
  select
    width_bucket(age_in_years, 20, 80, 6) as period,
    sum(churn_flag)                       as churn_count
  from {{ ref('fact_churn') }}
  group by 1
),

by_tenure as (
  select
    width_bucket(days_tenure, 0, 6000, 6) as period,
    sum(churn_flag)                       as churn_count
  from {{ ref('fact_churn') }}
  group by 1
),

by_premium as (
  select
    width_bucket(annual_premium, 0, 2000, 5) as period,
    sum(churn_flag)                         as churn_count
  from {{ ref('fact_churn') }}
  group by 1
)

select cast(period as varchar) as period, churn_count from monthly
union all
select cast(period as varchar) as period, churn_count from by_state
union all
select cast(period as varchar) as period, churn_count from by_age
union all
select cast(period as varchar) as period, churn_count from by_tenure
union all
select cast(period as varchar) as period, churn_count from by_premium
