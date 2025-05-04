
  
    

        create or replace transient table USER_DB_MACKEREL.RAW.stock_trend
         as
        (WITH moving_averages AS (
    SELECT 
        symbol_date_key,
        symbol,
        close,
        AVG(close) OVER (PARTITION BY symbol ORDER BY symbol_date_key ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma_5,
        AVG(close) OVER (PARTITION BY symbol ORDER BY symbol_date_key ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma_20
    FROM USER_DB_MACKEREL.RAW.stock_input
)
SELECT 
    symbol_date_key,
    symbol,
    close,
    ma_5,
    ma_20,
    CASE 
        WHEN ma_5 > ma_20 THEN 'uptrend'
        WHEN ma_5 < ma_20 THEN 'downtrend'
        ELSE 'sideways'
    END as trend
FROM moving_averages
WHERE ma_5 IS NOT NULL AND ma_20 IS NOT NULL
        );
      
  