WITH price_change AS (
    SELECT 
        symbol_date_key,
        symbol,
        close,
        close - LAG(close) OVER (PARTITION BY symbol ORDER BY symbol_date_key) as price_diff
    FROM USER_DB_MACKEREL.RAW.stock_input
),
gain_loss AS (
    SELECT 
        symbol_date_key,
        symbol,
        close,
        CASE WHEN price_diff > 0 THEN price_diff ELSE 0 END as gain,
        CASE WHEN price_diff < 0 THEN ABS(price_diff) ELSE 0 END as loss
    FROM price_change
),
avg_gain_loss AS (
    SELECT 
        symbol_date_key,
        symbol,
        close,
        AVG(gain) OVER (PARTITION BY symbol ORDER BY symbol_date_key ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_gain,
        AVG(loss) OVER (PARTITION BY symbol ORDER BY symbol_date_key ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) as avg_loss
    FROM gain_loss
)
SELECT 
    symbol_date_key,
    symbol,
    close,
    avg_gain,
    avg_loss,
    CASE 
        WHEN avg_loss = 0 THEN 100
        ELSE 100 - (100 / (1 + (avg_gain / avg_loss)))
    END as rsi
FROM avg_gain_loss
WHERE avg_gain IS NOT NULL AND avg_loss IS NOT NULL