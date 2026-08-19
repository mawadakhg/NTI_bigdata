import json
import os
import subprocess
from kafka import KafkaConsumer

KAFKA_SERVER = "localhost:9092"
TOPIC = "energy-data"

BATCH_SIZE = 100

HDFS_CONTAINER = "enerwise-namenode"
HDFS_DIR = "/enerwise/raw/energy"

TEMP_DIR = r"E:\EnerWise\consumer\temp"

GROUP_ID = "enerwise-hdfs-consumer-new"


SKIP_RECORDS = 0

os.makedirs(TEMP_DIR, exist_ok=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Connected to Kafka successfully!")
print(f"Reading from topic: {TOPIC}")

batch = []
batch_number = 1
records_seen = 0

for message in consumer:

    
    if records_seen < SKIP_RECORDS:
        records_seen += 1
        continue

    batch.append(message.value)
    records_seen += 1

    if len(batch) == BATCH_SIZE:

        file_name = f"batch_{batch_number:03d}.json"
        local_path = os.path.join(TEMP_DIR, file_name)

        with open(local_path, "w", encoding="utf-8") as f:
            json.dump(batch, f)

        subprocess.run(
            [
                "docker", "exec", HDFS_CONTAINER,
                "hdfs", "dfs", "-mkdir", "-p", HDFS_DIR
            ],
            check=True
        )

        subprocess.run(
            [
                "docker", "cp",
                local_path,
                f"{HDFS_CONTAINER}:/tmp/{file_name}"
            ],
            check=True
        )

        subprocess.run(
            [
                "docker", "exec", HDFS_CONTAINER,
                "hdfs", "dfs", "-put", "-f",
                f"/tmp/{file_name}",
                HDFS_DIR
            ],
            check=True
        )

        consumer.commit()

        print(
            f"Batch {batch_number} saved to HDFS: "
            f"{len(batch)} records"
        )

        os.remove(local_path)

        batch.clear()
        batch_number += 1

    
    if records_seen >= 5000:
       break

# Save remaining records
if batch:

    file_name = f"batch_{batch_number:03d}.json"
    local_path = os.path.join(TEMP_DIR, file_name)

    with open(local_path, "w", encoding="utf-8") as f:
        json.dump(batch, f)

    subprocess.run(
        [
            "docker", "cp",
            local_path,
            f"{HDFS_CONTAINER}:/tmp/{file_name}"
        ],
        check=True
    )

    subprocess.run(
        [
            "docker", "exec", HDFS_CONTAINER,
            "hdfs", "dfs", "-put", "-f",
            f"/tmp/{file_name}",
            HDFS_DIR
        ],
        check=True
    )

    consumer.commit()

    print(
        f"Batch {batch_number} saved to HDFS: "
        f"{len(batch)} records"
    )

    os.remove(local_path)

consumer.close()

print("Consumer finished successfully.")