

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
    USER_DB_MARMOT.dev.lab2_stock_data  -- Ensure the source name and table match your source definition