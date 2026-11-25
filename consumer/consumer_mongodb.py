import json
import os

from kafka import KafkaConsumer
from pymongo import MongoClient


TOPIC = os.getenv("KAFKA_TOPIC", "taxi-trips-valid")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
# 127.0.0.1 force la connexion vers le port publié par Docker sous Windows.
MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017/")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "binome1-mongodb-writer")

print("Connexion à MongoDB...")
client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
client.admin.command("ping")
collection = client["taxi_db"]["taxi_trips"]
collection.create_index(
    "trip_id",
    unique=True,
    partialFilterExpression={"trip_id": {"$type": "string"}},
)

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    group_id=GROUP_ID,
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
