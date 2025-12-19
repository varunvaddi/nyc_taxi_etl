WITH trips_with_row_num AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                pickup_location_id,
                taxi_type
            ORDER BY fare_amount, total_amount
        ) as row_num
    FROM {{ ref('int_all_trips_unified') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'vendor_id',
        'pickup_datetime',
        'dropoff_datetime',
        'pickup_location_id',
        'taxi_type',
        'row_num'
    ]) }} AS trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,
    taxi_type,
    trip_type,
    ehail_fee,
    trip_duration_minutes,
    trip_duration_seconds,
    pickup_date,
    pickup_hour,
    pickup_day_of_week,
    trip_distance_category,
    revenue_per_mile,
    revenue_per_minute
FROM trips_with_row_num