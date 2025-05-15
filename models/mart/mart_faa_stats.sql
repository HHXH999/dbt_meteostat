WITH flights AS (
    SELECT * FROM {{ ref('prep_flights') }}
),
airports AS (
    SELECT * FROM {{ ref('prep_airports') }}
),
-- 计算每个机场作为始发地的统计数据
origin_stats AS (
    SELECT
        origin AS airport_code,
        COUNT(*) AS total_departures_planned,
        COUNT(DISTINCT dest) AS unique_departure_connections,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_departures_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_departures_diverted,
        COUNT(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE NULL END) AS total_departures_actual,
        COUNT(DISTINCT tail_number) AS unique_departing_airplanes, -- 可选
        COUNT(DISTINCT airline) AS unique_departing_airlines -- 可选
    FROM flights
    GROUP BY 1
),
-- 计算每个机场作为目的地的统计数据
destination_stats AS (
    SELECT
        dest AS airport_code,
        COUNT(*) AS total_arrivals_planned,
        COUNT(DISTINCT origin) AS unique_arrival_connections,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_arrivals_cancelled, -- 注意：取消通常与始发地关联，这里可能需要根据业务逻辑调整
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_arrivals_diverted,
        COUNT(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE NULL END) AS total_arrivals_actual,
        COUNT(DISTINCT tail_number) AS unique_arriving_airplanes, -- 可选
        COUNT(DISTINCT airline) AS unique_arriving_airlines -- 可选
    FROM flights
    GROUP BY 1
),
-- 合并始发地和目的地统计
airport_flight_summary AS (
    SELECT
        COALESCE(os.airport_code, ds.airport_code) AS airport_code,
        COALESCE(os.unique_departure_connections, 0) AS unique_departure_connections,
        COALESCE(ds.unique_arrival_connections, 0) AS unique_arrival_connections,
        (COALESCE(os.total_departures_planned, 0) + COALESCE(ds.total_arrivals_planned, 0)) AS total_flights_planned,
        (COALESCE(os.total_departures_cancelled, 0) + COALESCE(ds.total_arrivals_cancelled, 0)) AS total_flights_cancelled, -- 再次注意取消逻辑
        (COALESCE(os.total_departures_diverted, 0) + COALESCE(ds.total_arrivals_diverted, 0)) AS total_flights_diverted,
        (COALESCE(os.total_departures_actual, 0) + COALESCE(ds.total_arrivals_actual, 0)) AS total_flights_actual
        -- 对于可选的飞机和航空公司统计，可以类似地合并或选择一个主要指标
    FROM origin_stats os
    FULL OUTER JOIN destination_stats ds ON os.airport_code = ds.airport_code
)
SELECT
    afs.airport_code,
    a.airport_name,
    a.city,
    a.country,
    a.region,
    afs.unique_departure_connections,
    afs.unique_arrival_connections,
    afs.total_flights_planned,
    afs.total_flights_cancelled,
    afs.total_flights_diverted,
    afs.total_flights_actual
FROM airport_flight_summary afs
JOIN airports a ON afs.airport_code = a.faa -- 假设 prep_airports 中的机场代码是 faa
ORDER BY afs.airport_code