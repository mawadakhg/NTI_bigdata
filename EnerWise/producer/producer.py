import pandas as pd
import json
from kafka import KafkaProducer
import time




FILE_PATH = r"E:\EnerWise\data\energy.csv"
KAFKA_SERVER = "localhost:9092"
TOPIC = "energy-data"

BATCH_SIZE = 100
BATCH_INTERVAL = 5  # seconds



df = pd.read_csv(FILE_PATH)

print("Data loaded successfully!")
print("Total rows:", len(df))





producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

print("Connected to Kafka successfully!")





for i in range(0, len(df), BATCH_SIZE):

    batch = df.iloc[i:i + BATCH_SIZE]

    # Send each row in the batch
    for _, row in batch.iterrows():
        producer.send(
            TOPIC,
            value=row.to_dict()
        )

    
    producer.flush()

    print(
        f"Batch {i // BATCH_SIZE + 1} sent: "
        f"{len(batch)} records"
    )

   
    if i + BATCH_SIZE < len(df):
        print(f"Waiting {BATCH_INTERVAL} seconds...")
        time.sleep(BATCH_INTERVAL)


producer.close()

print("All data sent successfully!")