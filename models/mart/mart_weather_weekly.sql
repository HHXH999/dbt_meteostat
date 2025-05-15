WITH daily_weather AS (
    SELECT
        date_year, -- 从 prep_weather_daily 获取
        cw AS calendar_week,  -- 从 prep_weather_daily 获取
        -- 根据指标特性选择合适的聚合函数
        avg_temp_c,
        min_temp_c,
        max_temp_c,
        precipitation_mm,
        max_snow_mm,
        avg_wind_direction, -- 对方向求平均可能意义不大，考虑求众数或特定条件下的风向
        avg_wind_speed_kmh,
        wind_peakgust_kmh,
        sun_minutes
        -- 添加 prep_weather_daily 中你希望按周聚合的其他天气指标
    FROM {{ ref('prep_weather_daily') }}
)
SELECT
    date_year,
    calendar_week,
    -- 温度相关：平均每日平均温，整周最低温，整周最高温
    AVG(avg_temp_c) AS weekly_avg_temp_c,
    MIN(min_temp_c) AS weekly_min_temp_c,
    MAX(max_temp_c) AS weekly_max_temp_c,

    -- 降水/降雪：总和
    SUM(precipitation_mm) AS weekly_total_precipitation_mm,
    SUM(max_snow_mm) AS weekly_total_snow_fall_mm,

    -- 风速：平均每日平均风速，整周最大风速，整周最大阵风
    AVG(avg_wind_speed_kmh) AS weekly_avg_wind_speed_kmh,
    MAX(avg_wind_speed_kmh) AS weekly_max_daily_avg_wind_speed_kmh, -- 一周中日平均风速的最大值
    MAX(wind_peakgust_kmh) AS weekly_max_wind_peakgust_kmh,

    -- 日照：总和
    SUM(sun_minutes) AS weekly_total_sun_minutes,

    -- 风向：计算众数可能比较复杂，这里可以先留空或选择其他方式
    -- MODE() WITHIN GROUP (ORDER BY avg_wind_direction) AS weekly_mode_wind_direction, -- PostgreSQL 的众数语法

    -- 计算包含特定天气现象的天数
    SUM(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END) AS weekly_days_with_precipitation,
    SUM(CASE WHEN max_snow_mm > 0 THEN 1 ELSE 0 END) AS weekly_days_with_snow,
    SUM(CASE WHEN sun_minutes > 0 THEN 1 ELSE 0 END) AS weekly_days_with_sunshine -- 假设日照分钟数大于0即为有日照

    -- 你可以根据需求添加更多基于每日数据的周聚合指标
FROM daily_weather
GROUP BY
    date_year,
    calendar_week
ORDER BY
    date_year,
    calendar_week