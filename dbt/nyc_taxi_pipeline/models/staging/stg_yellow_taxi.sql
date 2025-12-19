WITH source AS (
    SELECT * FROM {{ source('raw', 'YELLOW_TAXI') }}
),

cleaned AS (
    SELECT
        "VendorID" AS vendor_id,
        TO_TIMESTAMP("tpep_pickup_datetime" / 1000000) AS pickup_datetime,
        TO_TIMESTAMP("tpep_dropoff_datetime" / 1000000) AS dropoff_datetime,
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
        "Airport_fee" AS airport_fee,
        "cbd_congestion_fee" AS cbd_congestion_fee,
        NULL AS trip_type,
        NULL AS ehail_fee,
        'yellow' AS taxi_type
    FROM source
    WHERE "trip_distance" > 0
      AND "fare_amount" > 0
      AND "total_amount" > 0
      AND "passenger_count" > 0
      AND "tpep_pickup_datetime" < "tpep_dropoff_datetime"
)

SELECT * FROM cleaned