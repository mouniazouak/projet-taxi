import json
import time
import pandas as pd
from kafka import KafkaProducer

DATA_FILE = "data/yellow_tripdata_2024-01.parquet"
TOPIC = "taxi-trips-valid"
MAX_TRIPS = 100

columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_distance",
    "fare_amount",
    "passenger_count",
    "payment_type"
]

print("Lecture du fichier NYC TLC...")

df = pd.read_parquet(
    DATA_FILE,
    columns=columns
)

print(f"Lignes initiales : {len(df)}")

df["tpep_pickup_datetime"] = pd.to_datetime(
    df["tpep_pickup_datetime"],
    errors="coerce"
)

df["tpep_dropoff_datetime"] = pd.to_datetime(
    df["tpep_dropoff_datetime"],
    errors="coerce"
)

df = df.dropna(subset=columns)

df = df[
    (df["tpep_pickup_datetime"] >= "2024-01-01") &
    (df["tpep_pickup_datetime"] < "2024-02-01") &
    (df["tpep_dropoff_datetime"] >= df["tpep_pickup_datetime"]) &
    (df["trip_distance"] > 0) &
    (df["fare_amount"] >= 0) &
    (df["passenger_count"].between(1, 8))
]

df = df.sort_values("tpep_pickup_datetime")
df = df.head(MAX_TRIPS)

print(f"Lignes valides : {len(df)}")

if len(df) == 0:
    print("Aucune donnée valide trouvée.")
    exit()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda key: key.encode("utf-8"),
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)

for number, (_, row) in enumerate(df.iterrows(), start=1):

    message = {
        "trip_id": f"TLC_{number:06d}",
        "pickup_datetime": row["tpep_pickup_datetime"].isoformat(),
        "dropoff_datetime": row["tpep_dropoff_datetime"].isoformat(),
        "pickup_zone_id": int(row["PULocationID"]),
        "dropoff_zone_id": int(row["DOLocationID"]),
        "trip_distance_km": round(
            float(row["trip_distance"]) * 1.60934,
            2
        ),
        "fare_amount": round(float(row["fare_amount"]), 2),
        "passenger_count": int(row["passenger_count"]),
        "payment_type": str(row["payment_type"]),
        "status": "completed"
    }

    producer.send(
        TOPIC,
        key=message["trip_id"],
        value=message
    )

    print(f"Trajet envoyé : {message['trip_id']}")
    time.sleep(0.2)

producer.flush()
producer.close()

print("Envoi terminé.")