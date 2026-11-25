import json
import os

from kafka import KafkaConsumer
from pymongo import MongoClient


TOPIC = os.getenv("KAFKA_TOPIC", "taxi-trips-valid")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

print("Connexion à MongoDB...")
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
client.admin.command("ping")
collection = client["taxi_db"]["taxi_trips"]
collection.create_index("trip_id", unique=True)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id="binome1-mongodb-writer",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

print(f"Lecture du topic Kafka : {TOPIC}")

try:
    for message in consumer:
        trip = message.value
        trip_id = trip.get("trip_id")
        if not trip_id:
            print("Message ignoré : trip_id absent")
            continue
        collection.replace_one({"trip_id": trip_id}, trip, upsert=True)
        print(f"Course enregistrée : {trip_id}")
except KeyboardInterrupt:
    print("Arrêt demandé par l'utilisateur.")
finally:
    consumer.close()
    client.close()
    print("Consumer Kafka/MongoDB arrêté.")
