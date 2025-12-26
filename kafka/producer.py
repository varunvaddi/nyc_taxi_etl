import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import sys

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def stream_taxi_data(parquet_file, delay=0.1):
    """
    Read parquet file and stream records to Kafka topic
    delay: seconds between messages (0.1 = 10 messages/sec)
    """
    print(f"Reading {parquet_file}...")
    df = pd.read_parquet(parquet_file)
    
    # Convert to records
    records = df.head(1000).to_dict('records')  # Start with 1000 records for testing
    
    print(f"Streaming {len(records)} taxi trips to Kafka topic 'taxi-trips'...")
    
    for idx, record in enumerate(records):
        # Convert timestamps to strings for JSON serialization
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
            elif isinstance(value, (pd.Timestamp, datetime)):
                record[key] = str(value)
        
        # Send to Kafka
        producer.send('taxi-trips', value=record)
        
        if (idx + 1) % 100 == 0:
            print(f"Sent {idx + 1} messages...")
        
        time.sleep(delay)  # Simulate real-time streaming
    
    producer.flush()
    print(f"✅ Finished streaming {len(records)} records!")

if __name__ == "__main__":
    # Path to your yellow taxi parquet file
    parquet_path = "~/Desktop/SnowFlake/Data/NYC taxi/yellow_tripdata_2025-01.parquet" # UPDATE AS NEEDED--------------
    
    stream_taxi_data(parquet_path, delay=0.1)
