SELECT
    pickup_date,
    taxi_type,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(trip_duration_minutes) AS avg_trip_duration,
    AVG(fare_amount) AS avg_fare_amount,
    AVG(revenue_per_mile) AS avg_revenue_per_mile,
    AVG(revenue_per_minute) AS avg_revenue_per_minute,
    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
    SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips,
    AVG(tip_amount) AS avg_tip_amount,
    AVG(passenger_count) AS avg_passenger_count
FROM {{ ref('fct_trips') }}
GROUP BY 1, 2
ORDER BY 1 DESC, 2