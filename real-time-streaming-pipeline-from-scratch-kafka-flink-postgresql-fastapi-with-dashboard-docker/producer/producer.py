from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime, timezone

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value:
        json.dumps(value).encode("utf-8")
)

while True:
    event = {
        "device_id": f"D{random.randint(1, 20):04d}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": round(random.uniform(20, 80), 2),
        "pressure": round(random.uniform(990, 1030), 2),
        "battery": round(random.uniform(20, 100), 2)
    }
    producer.send(
        "sensor-events",
        value=event
    )
    print(event)
    time.sleep(0.2)
