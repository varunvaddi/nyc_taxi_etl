WITH yellow AS (
    SELECT * FROM {{ ref('stg_yellow_taxi') }}
),

green AS (
    SELECT * FROM {{ ref('stg_green_taxi') }}
),

unified AS (
    SELECT * FROM yellow
    UNION ALL
    SELECT * FROM green
),

enriched AS (
    SELECT
        *,
        DATEDIFF('minute', pickup_datetime, dropoff_datetime) AS trip_duration_minutes,
        DATEDIFF('second', pickup_datetime, dropoff_datetime) AS trip_duration_seconds,
        DATE(pickup_datetime) AS pickup_date,
        HOUR(pickup_datetime) AS pickup_hour,
        DAYOFWEEK(pickup_datetime) AS pickup_day_of_week,
        CASE 
            WHEN trip_distance <= 2 THEN 'short'
            WHEN trip_distance <= 10 THEN 'medium'
            ELSE 'long'
        END AS trip_distance_category,
        fare_amount / NULLIF(trip_distance, 0) AS revenue_per_mile,
        fare_amount / NULLIF(trip_duration_minutes, 0) AS revenue_per_minute
    FROM unified
)

SELECT *
FROM enriched
WHERE trip_duration_minutes > 0
  AND trip_duration_minutes < 1440