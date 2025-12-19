SELECT
    {{ dbt_utils.generate_surrogate_key([
    'pickup_datetime',
    'dropoff_datetime',
    'pickup_location_id',
    'taxi_type'
    ]) 
    }} AS trip_id, *
FROM {{ ref('int_all_trips_unified') }}