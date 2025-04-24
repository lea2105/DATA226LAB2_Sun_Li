{{ config(materialized='table') }}

-- Select relevant columns from the stock data source
select
    symbol,
    date,
    open,
    close,
    high,
    low,
    volume
from
    {{ source('dev', 'lab2_stock_data') }}  -- Ensure the source name and table match your source definition
