WITH daily_weather AS (
    SELECT
        station_id, -- 或者 airport_code，取决于 prep_weather_daily 如何关联到机场
        date,
        min_temp_c,
        max_temp_c,
        precipitation_mm,
        max_snow_mm AS snow_fall_mm, -- 假设 max_snow_mm 代表每日降雪
        avg_wind_direction,
        avg_wind_speed_kmh,
        wind_peakgust_kmh
    FROM {{ ref('prep_weather_daily') }}
),
flights AS (
    SELECT * FROM {{ ref('prep_flights') }}
),
airports AS (
    SELECT * FROM {{ ref('prep_airports') }} -- 用于机场名称、城市等
),
-- 每日每个机场的始发航班统计
daily_origin_stats AS (
    SELECT
        flight_date AS date,
        origin AS airport_code,
        COUNT(*) AS total_departures_planned,
        COUNT(DISTINCT dest) AS unique_departure_connections,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_departures_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_departures_diverted,
        COUNT(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE NULL END) AS total_departures_actual
        -- 可选的飞机和航空公司统计可以类似添加
    FROM flights
    GROUP BY 1, 2
),
-- 每日每个机场的目的航班统计
daily_destination_stats AS (
    SELECT
        flight_date AS date,
        dest AS airport_code,
        COUNT(*) AS total_arrivals_planned,
        COUNT(DISTINCT origin) AS unique_arrival_connections,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS total_arrivals_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END) AS total_arrivals_diverted,
        COUNT(CASE WHEN cancelled = 0 AND diverted = 0 THEN 1 ELSE NULL END) AS total_arrivals_actual
    FROM flights
    GROUP BY 1, 2
),
-- 每日每个机场的航班汇总
daily_airport_flight_summary AS (
    SELECT
        COALESCE(dos.date, dds.date) AS date,
        COALESCE(dos.airport_code, dds.airport_code) AS airport_code,
        COALESCE(dos.unique_departure_connections, 0) AS unique_departure_connections,
        COALESCE(dds.unique_arrival_connections, 0) AS unique_arrival_connections,
        (COALESCE(dos.total_departures_planned, 0) + COALESCE(dds.total_arrivals_planned, 0)) AS total_flights_planned,
        (COALESCE(dos.total_departures_cancelled, 0) + COALESCE(dds.total_arrivals_cancelled, 0)) AS total_flights_cancelled,
        (COALESCE(dos.total_departures_diverted, 0) + COALESCE(dds.total_arrivals_diverted, 0)) AS total_flights_diverted,
        (COALESCE(dos.total_departures_actual, 0) + COALESCE(dds.total_arrivals_actual, 0)) AS total_flights_actual
    FROM daily_origin_stats dos
    FULL OUTER JOIN daily_destination_stats dds ON dos.date = dds.date AND dos.airport_code = dds.airport_code
)
SELECT
    fs.date,
    fs.airport_code,
    a.airport_name, -- 可选
    a.city,         -- 可选
    a.country,      -- 可选
    fs.unique_departure_connections,
    fs.unique_arrival_connections,
    fs.total_flights_planned,
    fs.total_flights_cancelled,
    fs.total_flights_diverted,
    fs.total_flights_actual,
    dw.min_temp_c,
    dw.max_temp_c,
    dw.precipitation_mm,
    dw.snow_fall_mm,
    dw.avg_wind_direction,
    dw.avg_wind_speed_kmh,
    dw.wind_peakgust_kmh
FROM daily_airport_flight_summary fs
JOIN airports a ON fs.airport_code = a.faa -- 连接机场信息 (如果需要名称等)
JOIN daily_weather dw ON fs.airport_code = dw.station_id::VARCHAR AND fs.date = dw.date -- 连接天气数据
-- WHERE dw.station_id IS NOT NULL -- 这一行确保只选择有天气数据的机场和日期组合，因为是 INNER JOIN，所以如果天气数据没有匹配，该行航班数据也不会出现
ORDER BY
    fs.date,
    fs.airport_code