{{
    config(
        materialized='incremental',
        unique_key='pickup_datetime'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'GREEN_TAXI') }}
    {% if is_incremental() %}
    WHERE TO_TIMESTAMP("lpep_pickup_datetime" / 1000000) > (SELECT MAX(pickup_datetime) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        "VendorID" AS vendor_id,
        TO_TIMESTAMP("lpep_pickup_datetime" / 1000000) AS pickup_datetime,
        TO_TIMESTAMP("lpep_dropoff_datetime" / 1000000) AS dropoff_datetime,
        "passenger_count" AS passenger_count,
        "trip_distance" AS trip_distance,
        "RatecodeID" AS rate_code_id,
        "store_and_fwd_flag" AS store_and_fwd_flag,
        "PULocationID" AS pickup_location_id,
        "DOLocationID" AS dropoff_location_id,
        "payment_type" AS payment_type,
        "fare_amount" AS fare_amount,
        "extra" AS extra,
        "mta_tax" AS mta_tax,
        "tip_amount" AS tip_amount,
        "tolls_amount" AS tolls_amount,
        "improvement_surcharge" AS improvement_surcharge,
        "total_amount" AS total_amount,
        "congestion_surcharge" AS congestion_surcharge,
        "trip_type" AS trip_type,
        "ehail_fee" AS ehail_fee,
        "cbd_congestion_fee" AS cbd_congestion_fee,
        NULL AS airport_fee,
        'green' AS taxi_type
    FROM source
    WHERE "trip_distance" > 0
      AND "fare_amount" > 0
      AND "total_amount" > 0
      AND "passenger_count" > 0
      AND "lpep_pickup_datetime" < "lpep_dropoff_datetime"
)

SELECT * FROM cleaned