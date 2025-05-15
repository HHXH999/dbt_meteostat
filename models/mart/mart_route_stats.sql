WITH flights AS (
    SELECT * FROM {{ ref('prep_flights') }}
),
airports AS (
    SELECT * FROM {{ ref('prep_airports') }} -- 用于获取机场名称、城市等信息
),
route_aggregated_stats AS (
    SELECT
        origin AS origin_airport_code,
        dest AS destination_airport_code,
        COUNT(*) AS total_flights_on_route,
        COUNT(DISTINCT tail_number) AS unique_airplanes_on_route,
        COUNT(DISTINCT airline) AS unique_airlines_on_route,
        AVG(actual_elapsed_time) AS avg_actual_elapsed_time_minutes, -- 假设 actual_elapsed_time 是分钟数
        AVG(arr_delay) AS avg_arrival_delay_minutes,                 -- 假设 arr_delay 是分钟数
        MAX(arr_delay) AS max_arrival_delay_minutes,
        MIN(arr_delay) AS min_arrival_delay_minutes,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled_on_route,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_diverted_on_route
    FROM flights
    GROUP BY
        origin,
        dest
)
SELECT
    rs.origin_airport_code,
    oa.airport_name AS origin_airport_name,
    oa.city AS origin_city,
    oa.country AS origin_country,
    oa.region AS origin_region,
    rs.destination_airport_code,
    da.airport_name AS destination_airport_name,
    da.city AS destination_city,
    da.country AS destination_country,
    da.region AS destination_region,
    rs.total_flights_on_route,
    rs.unique_airplanes_on_route,
    rs.unique_airlines_on_route,
    rs.avg_actual_elapsed_time_minutes,
    -- 如果你的 prep_flights 中有 *_interval 类型，可以直接求平均值，结果也是 interval
    -- 例如 AVG(actual_elapsed_time_interval) AS avg_actual_elapsed_time_interval
    rs.avg_arrival_delay_minutes,
    rs.max_arrival_delay_minutes,
    rs.min_arrival_delay_minutes,
    rs.total_cancelled_on_route,
    rs.total_diverted_on_route
FROM route_aggregated_stats rs
JOIN airports oa ON rs.origin_airport_code = oa.faa -- 连接始发机场信息
JOIN airports da ON rs.destination_airport_code = da.faa -- 连接目的地机场信息
ORDER BY
    rs.origin_airport_code,
    rs.destination_airport_code