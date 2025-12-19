SELECT
    payment_type,
    taxi_type,
    trip_distance_category,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(fare_amount) AS avg_fare,
    AVG(tip_amount) AS avg_tip,
    AVG(tip_amount) / NULLIF(AVG(fare_amount), 0) * 100 AS avg_tip_percentage
FROM {{ ref('fct_trips') }}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3