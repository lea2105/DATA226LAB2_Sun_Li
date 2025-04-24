-- -- lab2_stock_data_seed.sql
-- {{ config(
--     materialized='seed'
-- ) }}

-- -- In case of using a DBT seed file, you can have a SQL seed file like this:
-- -- For example, a CSV file that is already loaded in your DBT project
-- SELECT * FROM {{ ref('lab2_stock_data_seed') }};
