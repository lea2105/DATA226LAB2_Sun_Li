{% snapshot lab2_stock_moving_avg_snapshot %}

{{
  config(
    target_schema='snapshot',
    unique_key=['symbol', 'date'],
    strategy='check',
    check_cols=['close']
  )
}}

SELECT 
    *,
    ROW_NUMBER() OVER (PARTITION BY symbol, date ORDER BY date) AS row_id
FROM {{ source('dev', 'lab2_stock_data') }}

{% endsnapshot %}

