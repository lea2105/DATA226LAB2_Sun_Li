select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select moving_avg
from USER_DB_MARMOT.dev.lab2_stock_moving_avg
where moving_avg is null



      
    ) dbt_internal_test