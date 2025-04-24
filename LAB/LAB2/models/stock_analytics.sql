{{ config(
    materialized='table',
    schema='ANALYTICS',
    alias='STOCK_ANALYTICS',
    unique_key=['SYMBOL', 'DATE']
) }}

WITH raw_data AS (
    SELECT
        SYMBOL,
        DATE,
        CLOSE
    FROM {{ source('RAW', 'stock_price') }}
),
forecast_data AS (
    SELECT
        SYMBOL,
        DATE,
        FORECAST,
        LOWER_BOUND,
        UPPER_BOUND
    FROM {{ source('ANALYTICS', 'market_data') }}
    WHERE FORECAST IS NOT NULL
),
moving_averages AS (
    SELECT
        r.SYMBOL,
        r.DATE,
        r.CLOSE,
        AVG(r.CLOSE) OVER (
            PARTITION BY r.SYMBOL
            ORDER BY r.DATE
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS MOVING_AVG_7D,
        AVG(r.CLOSE) OVER (
            PARTITION BY r.SYMBOL
            ORDER BY r.DATE
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS MOVING_AVG_14D
    FROM raw_data r
),
rsi_calc AS (
    SELECT
        SYMBOL,
        DATE,
        CLOSE,
        CLOSE - LAG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DATE) AS price_change,
        CASE
            WHEN CLOSE - LAG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DATE) > 0
            THEN CLOSE - LAG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DATE)
            ELSE 0
        END AS gain,
        CASE
            WHEN CLOSE - LAG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DATE) < 0
            THEN ABS(CLOSE - LAG(CLOSE) OVER (PARTITION BY SYMBOL ORDER BY DATE))
            ELSE 0
        END AS loss
    FROM raw_data
),
avg_gains_losses AS (
    SELECT
        SYMBOL,
        DATE,
        CLOSE,
        AVG(gain) OVER (
            PARTITION BY SYMBOL
            ORDER BY DATE
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_gain,
        CASE 
            WHEN AVG(loss) OVER (
                PARTITION BY SYMBOL
                ORDER BY DATE
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) = 0 THEN 1
            ELSE AVG(loss) OVER (
                PARTITION BY SYMBOL
                ORDER BY DATE
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            )
        END AS avg_loss
    FROM rsi_calc
),
rsi_14d AS (
    SELECT
        SYMBOL,
        DATE,
        CLOSE,
        CASE
            WHEN avg_gain = 0 AND avg_loss = 0 THEN 50
            ELSE LEAST(100, GREATEST(0, 
                100 - (100 / (1 + (avg_gain / avg_loss)))
            ))
        END AS RSI_14D
    FROM avg_gains_losses
    WHERE DATE >= (SELECT MIN(DATE) + INTERVAL '14 DAY' FROM raw_data)
)
SELECT
    m.SYMBOL,
    m.DATE,
    m.CLOSE,
    m.MOVING_AVG_7D,
    m.MOVING_AVG_14D,
    r.RSI_14D,
    f.FORECAST,
    f.LOWER_BOUND,
    f.UPPER_BOUND
FROM moving_averages m
LEFT JOIN rsi_14d r
    ON m.SYMBOL = r.SYMBOL AND m.DATE = r.DATE
LEFT JOIN forecast_data f
    ON m.SYMBOL = f.SYMBOL AND m.DATE = f.DATE
ORDER BY m.SYMBOL, m.DATE