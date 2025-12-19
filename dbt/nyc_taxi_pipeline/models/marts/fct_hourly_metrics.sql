SELECT
    pickup_date,
    pickup_hour,
    taxi_type,
    COUNT(*) AS total_trips,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(trip_duration_minutes) AS avg_trip_duration,
    AVG(passenger_count) AS avg_passenger_count,
    SUM(CASE WHEN payment_type = 1 THEN 1 ELSE 0 END) AS credit_card_trips,
    SUM(CASE WHEN payment_type = 2 THEN 1 ELSE 0 END) AS cash_trips,
    AVG(tip_amount) AS avg_tip_amount,
    AVG(revenue_per_mile) AS avg_revenue_per_mile
FROM {{ ref('fct_trips') }}
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2 DESC, 3