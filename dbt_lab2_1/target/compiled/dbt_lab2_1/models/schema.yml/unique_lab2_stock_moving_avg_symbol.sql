
    
    

select
    symbol as unique_field,
    count(*) as n_records

from USER_DB_MARMOT.dev.lab2_stock_moving_avg
where symbol is not null
group by symbol
having count(*) > 1


