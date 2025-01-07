from kafka import KafkaProducer
import time
import pandas as pd
import json

print('hello world')

# Retry logic for Kafka connection
for attempt in range(5):  # Retry 5 times
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka producer connected.")
        break
    except Exception as e:
        print(f"Attempt {attempt + 1}: Kafka not available yet. Retrying in 5 seconds...")
        time.sleep(5)
else:
    raise Exception("Could not connect to Kafka after 5 attempts.")

# Load batch data
data = pd.read_csv('your_dataset.csv')

# Stream data row by row
for _, row in data.iterrows():
    producer.send('your_topic', value=row.to_dict())
    print(f"Sent: {row.to_dict()}")
    time.sleep(1)  # Simulates real-time data
