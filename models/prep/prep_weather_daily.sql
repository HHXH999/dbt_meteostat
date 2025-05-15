WITH daily_data AS (
    SELECT *
    FROM {{ ref('staging_weather_daily') }} -- 引用 staging 层的每日天气模型
),
add_features AS (
    SELECT
        *,
        DATE_PART('day', date)::INTEGER AS date_day,       -- 月份中的第几天
        DATE_PART('month', date)::INTEGER AS date_month,   -- 年份中的第几个月
        DATE_PART('year', date)::INTEGER AS date_year,     -- 年份
        DATE_PART('week', date)::INTEGER AS cw,            -- 年份中的第几周 (ISO week)
        TO_CHAR(date, 'FMMonth') AS month_name,      -- 月份的完整名称 (e.g., March). FM  (https://www.postgresql.org/docs/current/functions-formatting.html#FUNCTIONS-FORMATTING-DATETIME-TABLE ) 来去除月份名称后面的空格
        TO_CHAR(date, 'FMDay') AS weekday          -- 星期的完整名称 (e.g., Wednesday)
    FROM daily_data
),
add_more_features AS (
    SELECT
        *,
        (CASE
            WHEN month_name IN ('December', 'January', 'February') THEN 'winter'
            WHEN month_name IN ('March', 'April', 'May') THEN 'spring'
            WHEN month_name IN ('June', 'July', 'August') THEN 'summer'
            WHEN month_name IN ('September', 'October', 'November') THEN 'autumn'
            ELSE NULL -- 以防万一 month_name 不是预期的格式
        END) AS season
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY date

-- DATE_PART() 函数提取日、月、年、周。::INTEGER 用于将结果转换为整数。
-- TO_CHAR() 函数将日期转换为月份名称 ('FMMonth') 和星期名称 ('FMDay')。FM 前缀用于移除可能由某些数据库（如 PostgreSQL）在月份或星期名称后添加的填充空格。