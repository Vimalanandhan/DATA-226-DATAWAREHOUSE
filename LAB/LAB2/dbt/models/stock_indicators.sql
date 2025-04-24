WITH daily_return AS (
    SELECT 
        symbol_date_key,
        symbol,
        close,
        (close - LAG(close) OVER (PARTITION BY symbol ORDER BY symbol_date_key)) / LAG(close) OVER (PARTITION BY symbol ORDER BY symbol_date_key) as daily_price_return
    FROM {{ source('stock_data', 'stock_input') }}
)
SELECT 
    i.symbol_date_key,
    i.symbol,
    i.close,
    t.ma_5,
    t.ma_20,
    t.trend,
    r.avg_gain,
    r.avg_loss,
    r.rsi,
    d.daily_price_return
FROM {{ source('stock_data', 'stock_input') }} i
LEFT JOIN {{ ref('stock_trend') }} t ON i.symbol_date_key = t.symbol_date_key
LEFT JOIN {{ ref('stock_rsi') }} r ON i.symbol_date_key = r.symbol_date_key
LEFT JOIN daily_return d ON i.symbol_date_key = d.symbol_date_key 