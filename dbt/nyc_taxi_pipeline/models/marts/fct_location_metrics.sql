SELECT
    pickup_location_id,
    dropoff_location_id,
    taxi_type,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(trip_duration_minutes) AS avg_trip_duration,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip
FROM {{ ref('fct_trips') }}
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 10  -- Only include routes with at least 10 trips
ORDER BY total_trips DESC