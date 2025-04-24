WITH daily AS (
    SELECT * FROM {{ ref("daily_price_return") }}
),
ma as (select ma_5, ma_20, date,symbol from {{ref("stock_ma")}}),
trend as (select trend, date, symbol from {{ref("stock_trend")}}),
rsi as (select avg_gain, avg_loss, RSI, date, symbol from {{ref("stock_rsi")}})

SELECT 
daily.*, ma.ma_5, ma.ma_20, trend.trend, rsi.avg_gain, rsi.avg_loss, rsi.RSI
FROM 
daily join ma on daily.date = ma.date and daily.symbol = ma.symbol
join trend on daily.date = trend.date and daily.symbol = trend.symbol
join rsi on daily.date = rsi.date and daily.symbol = rsi.symbol
ORDER BY 
    date,symbol 