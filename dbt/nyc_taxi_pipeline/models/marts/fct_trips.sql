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
    *
FROM trips_with_row_num