import json
import snowflake.connector
from kafka import KafkaConsumer
from datetime import datetime

# Snowflake connection
conn = snowflake.connector.connect(
    account='RKRTMUP-PC79881',
    user='vvaddi2',
    password='Shinigami#151230',
    warehouse='COMPUTE_WH',
    database='NYC_TAXI',
    role='ACCOUNTADMIN'  # Use ACCOUNTADMIN role
)

cursor = conn.cursor()

# Create streaming schema if not exists
cursor.execute("CREATE SCHEMA IF NOT EXISTS STREAMING")
cursor.execute("USE SCHEMA STREAMING")

# Create real-time staging table
cursor.execute("""
CREATE TABLE IF NOT EXISTS YELLOW_TAXI_STREAMING (
    VendorID INTEGER,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance FLOAT,
    RatecodeID INTEGER,
    store_and_fwd_flag STRING,
    PULocationID INTEGER,
    DOLocationID INTEGER,
    payment_type INTEGER,
    fare_amount FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tip_amount FLOAT,
    tolls_amount FLOAT,
    improvement_surcharge FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    Airport_fee FLOAT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
""")

print("✅ Created STREAMING.YELLOW_TAXI_STREAMING table")

# Kafka consumer
consumer = KafkaConsumer(
    'taxi-trips',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='snowflake-consumer'
)

print("🚀 Consuming messages from Kafka and writing to Snowflake...")

count = 0
for message in consumer:
    trip = message.value
    
    # Insert into Snowflake
    cursor.execute("""
        INSERT INTO STREAMING.YELLOW_TAXI_STREAMING (
            VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
            passenger_count, trip_distance, RatecodeID, store_and_fwd_flag,
            PULocationID, DOLocationID, payment_type, fare_amount,
            extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
            total_amount, congestion_surcharge, Airport_fee
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        trip.get('VendorID'),
        trip.get('tpep_pickup_datetime'),
        trip.get('tpep_dropoff_datetime'),
        trip.get('passenger_count'),
        trip.get('trip_distance'),
        trip.get('RatecodeID'),
        trip.get('store_and_fwd_flag'),
        trip.get('PULocationID'),
        trip.get('DOLocationID'),
        trip.get('payment_type'),
        trip.get('fare_amount'),
        trip.get('extra'),
        trip.get('mta_tax'),
        trip.get('tip_amount'),
        trip.get('tolls_amount'),
        trip.get('improvement_surcharge'),
        trip.get('total_amount'),
        trip.get('congestion_surcharge'),
        trip.get('Airport_fee')
    ))
    
    count += 1
    if count % 100 == 0:
        conn.commit()
        print(f"✅ Inserted {count} records into Snowflake")

cursor.close()
conn.close()