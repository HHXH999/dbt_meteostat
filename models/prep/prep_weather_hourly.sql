WITH hourly_data AS (
    SELECT *
    FROM {{ ref('staging_weather_hourly') }} -- Referencing the hourly staging model
),
add_features AS (
    SELECT
        *,
        timestamp::DATE AS date,                      -- Extract only date
        timestamp::TIME AS time,                      -- Extract only time
        TO_CHAR(timestamp, 'HH24:MI') AS hour,        -- Time as HH:MI text
        TO_CHAR(timestamp, 'FMMonth') AS month_name,  -- Full month name
        TO_CHAR(timestamp, 'FMDay') AS weekday,       -- Full weekday name
        DATE_PART('day', timestamp)::INTEGER AS date_day,
        DATE_PART('month', timestamp)::INTEGER AS date_month,
        DATE_PART('year', timestamp)::INTEGER AS date_year,
        DATE_PART('week', timestamp)::INTEGER AS cw
    FROM hourly_data
),
add_more_features AS (
    SELECT
        *,
        (CASE
            WHEN time >= '00:00:00'::TIME AND time < '06:00:00'::TIME THEN 'night'
            WHEN time >= '06:00:00'::TIME AND time < '18:00:00'::TIME THEN 'day'
            WHEN time >= '18:00:00'::TIME AND time <= '23:59:59'::TIME THEN 'evening'
            ELSE 'unknown' -- Fallback for any unexpected cases
        END) AS day_part
    FROM add_features
)
SELECT *
FROM add_more_features
ORDER BY timestamp

