-- IMPORTANT: Verify and adjust the column list below based on your staging_airports table structure!
-- You can find the exact column names by running "SELECT * FROM {{ ref('staging_airports') }} LIMIT 1" in DBeaver or dbt Cloud.

WITH airports_reordered AS (
    SELECT
        faa,                
        name AS airport_name,
        city,
        country,             
        region,             
        lat AS latitude,
        lon AS longitude,
        alt AS altitude,
        tz AS timezone,
        dst AS daylight_saving_time
        -- Add any other columns from your staging_airports table here, in your desired order
    FROM {{ ref('staging_airports') }}
)
SELECT * FROM airports_reorder