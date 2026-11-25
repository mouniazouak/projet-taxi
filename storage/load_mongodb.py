import pandas as pd
from pymongo import MongoClient

print("Lecture du fichier traité...")

df = pd.read_parquet("data/processed_trips.parquet")

df["tpep_pickup_datetime"] = df["tpep_pickup_datetime"].astype(str)
df["PULocationID"] = df["PULocationID"].fillna(0).astype(int)
df["trip_distance"] = df["trip_distance"].fillna(0).astype(float)
df["fare_amount"] = df["fare_amount"].fillna(0).astype(float)
df["passenger_count"] = df["passenger_count"].fillna(0).astype(int)

df = df.where(pd.notnull(df), None)
documents = df.to_dict("records")

client = MongoClient("mongodb://localhost:27017/")

db = client["taxi_db"]
collection = db["taxi_trips"]

collection.delete_many({})
collection.insert_many(documents)

collection.create_index("PULocationID")
collection.create_index("pickup_borough")

print("Connexion à MongoDB réussie.")
print("Documents enregistrés :", collection.count_documents({}))

client.close()