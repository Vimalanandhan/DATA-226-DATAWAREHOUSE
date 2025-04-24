SELECT
    *, CONCAT(symbol,'_', date) as symbol_date_id
FROM {{ source('raw_data', 'stock_table') }}