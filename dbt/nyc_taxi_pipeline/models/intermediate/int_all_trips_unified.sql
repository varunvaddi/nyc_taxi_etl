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
        -- Improved revenue per mile with edge case handling
        CASE 
            WHEN trip_distance < 0.1 THEN NULL  -- Too short to calculate meaningful RPM
            ELSE fare_amount / NULLIF(trip_distance, 0)
        END AS revenue_per_mile,
        CASE 
            WHEN trip_duration_minutes < 1 THEN NULL  -- Too short to calculate meaningful RPM
            ELSE fare_amount / NULLIF(trip_duration_minutes, 0)
        END AS revenue_per_minute,
        -- Flag for data quality monitoring
        CASE 
            WHEN trip_distance < 0.1 AND fare_amount > 20 THEN 'short_high_fare'
            WHEN trip_distance > 50 AND trip_duration_minutes > 300 THEN 'long_suspicious'
            WHEN fare_amount / NULLIF(trip_distance, 0) > 100 THEN 'high_rpm_outlier'
            WHEN fare_amount / NULLIF(trip_distance, 0) < 1 THEN 'low_rpm_outlier'
            ELSE 'normal'
        END AS data_quality_flag
    FROM unified
)

SELECT *
FROM enriched
WHERE trip_duration_minutes > 0
  AND trip_duration_minutes < 1440