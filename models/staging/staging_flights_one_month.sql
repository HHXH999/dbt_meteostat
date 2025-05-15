{{ config(materialized='view') }} -- 覆盖 dbt_project.yml 中的默认设置，将这个特定的模型物化为视图。

WITH flights_one_month AS (
    SELECT * 
    FROM {{source('flights_data', 'flights')}}
    WHERE DATE_PART('month', flight_date) = 1 
)
SELECT * FROM flights_one_monthgit