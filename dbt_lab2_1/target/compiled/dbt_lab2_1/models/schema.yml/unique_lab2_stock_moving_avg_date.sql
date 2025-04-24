
    
    

select
    date as unique_field,
    count(*) as n_records

from USER_DB_MARMOT.dev.lab2_stock_moving_avg
where date is not null
group by date
having count(*) > 1


