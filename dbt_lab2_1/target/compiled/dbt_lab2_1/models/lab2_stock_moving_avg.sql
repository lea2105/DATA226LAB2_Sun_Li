SELECT
    symbol,
    date,
    close,
    AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7,
    AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ma_30
FROM USER_DB_MARMOT.dev.lab2_stock_data
ORDER BY symbol, date