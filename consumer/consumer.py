import json
from kafka import KafkaConsumer

TOPIC = "taxi-trips-valid"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="binome1-consumer",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    )
)

print("Consommateur Kafka démarré...")

count = 0

for message in consumer:
    print(json.dumps(message.value, indent=2))
    count += 1

    if count >= 10:
        break

consumer.close()

print("Consommation terminée.")