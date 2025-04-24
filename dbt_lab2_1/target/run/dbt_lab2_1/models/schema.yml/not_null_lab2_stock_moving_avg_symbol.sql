select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select symbol
from USER_DB_MARMOT.dev.lab2_stock_moving_avg
where symbol is null



      
    ) dbt_internal_test