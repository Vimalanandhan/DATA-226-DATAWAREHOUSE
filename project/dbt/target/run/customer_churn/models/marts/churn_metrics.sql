
  
    

        create or replace transient table USER_DB_FALCON.RAW_public.churn_metrics
         as
        (WITH 
  monthly AS (
    SELECT
      DATE_TRUNC('month', churn_date) AS period,
      COUNT(*) AS churn_count
    FROM USER_DB_FALCON.RAW_public.fact_churn
    GROUP BY 1
  ),
  
  by_state AS (
    SELECT
      state AS period,
      COUNT(*) AS churn_count
    FROM USER_DB_FALCON.RAW_public.fact_churn
    GROUP BY 1
  ),
  
  by_age AS (
    SELECT
      CASE
        WHEN age_in_years < 25 THEN 'Under 25'
        WHEN age_in_years < 35 THEN '25-34'
        WHEN age_in_years < 45 THEN '35-44'
        WHEN age_in_years < 55 THEN '45-54'
        WHEN age_in_years < 65 THEN '55-64'
        ELSE '65+'
      END AS period,
      COUNT(*) AS churn_count
    FROM USER_DB_FALCON.RAW_public.fact_churn
    GROUP BY 1
  ),
  
  by_tenure AS (
    SELECT
      CASE
        WHEN days_tenure <= 90 THEN '0-3 months'
        WHEN days_tenure <= 180 THEN '4-6 months'
        WHEN days_tenure <= 365 THEN '7-12 months'
        WHEN days_tenure <= 730 THEN '13-24 months'
        ELSE '25+ months'
      END AS period,
      COUNT(*) AS churn_count
    FROM USER_DB_FALCON.RAW_public.fact_churn
    GROUP BY 1
  ),
  
  by_premium AS (
    SELECT
      CASE
        WHEN annual_premium < 1000 THEN 'Under $1,000'
        WHEN annual_premium < 2000 THEN '$1,000-$1,999'
        WHEN annual_premium < 3000 THEN '$2,000-$2,999'
        ELSE '$3,000+'
      END AS period,
      COUNT(*) AS churn_count
    FROM USER_DB_FALCON.RAW_public.fact_churn
    GROUP BY 1
  )

SELECT
  CAST(period AS VARCHAR) AS period,
  churn_count,
  'monthly'              AS category
FROM monthly

UNION ALL

SELECT
  CAST(period AS VARCHAR) AS period,
  churn_count,
  'by_state'             AS category
FROM by_state

UNION ALL

SELECT
  CAST(period AS VARCHAR) AS period,
  churn_count,
  'by_age'               AS category
FROM by_age

UNION ALL

SELECT
  CAST(period AS VARCHAR) AS period,
  churn_count,
  'by_tenure'            AS category
FROM by_tenure

UNION ALL

SELECT
  CAST(period AS VARCHAR) AS period,
  churn_count,
  'by_premium'           AS category
FROM by_premium
        );
      
  