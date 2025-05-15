WITH flights_one_month AS (
    SELECT *
    FROM {{ ref('staging_flights_one_month') }} -- 引用 staging 层的单月航班数据模型
),
flights_cleaned AS (
    SELECT
        flight_date::DATE AS flight_date, -- 确保 flight_date 是 DATE 类型
        TO_CHAR(dep_time, 'fm0000')::TIME AS dep_time, -- 将数字转换为 HHMM 格式的字符串，再转为 TIME
        TO_CHAR(sched_dep_time, 'fm0000')::TIME AS sched_dep_time,
        dep_delay,
        (dep_delay * '1 minute'::INTERVAL) AS dep_delay_interval, -- 将分钟数转换为 INTERVAL
        TO_CHAR(arr_time, 'fm0000')::TIME AS arr_time,
        TO_CHAR(sched_arr_time, 'fm0000')::TIME AS sched_arr_time,
        arr_delay,
        (arr_delay * INTERVAL '1 minute') AS arr_delay_interval,
        airline,
        tail_number,
        flight_number,
        origin,
        dest,
        air_time,
        (air_time * INTERVAL '1 minute') AS air_time_interval,
        actual_elapsed_time,
        (actual_elapsed_time * INTERVAL '1 minute') AS actual_elapsed_time_interval,
        (distance / 0.621371)::NUMERIC(6,2) AS distance_km,  
        cancelled,
        diverted
    FROM flights_one_month
)
SELECT *
FROM flights_cleaned
ORDER BY flight_date

-- 'fm0000' 格式化字符串：0000 表示将数字补全到4位数（不足则前面补0），fm 用于去除可能由某些数据库在转换时添加的前导空格或0。
-- ::TIME 将格式化后的字符串 (如 "0830" 或 "1705") 转换为标准的时间类型。如果你的原始时间格式不同 (例如已经是 "08:30")，你需要调整 TO_CHAR 或直接进行类型转换。