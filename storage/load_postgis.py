import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

INPUT_FILE = "data/processed_trips.parquet"

print("Lecture du fichier traité...")

df = pd.read_parquet(INPUT_FILE)

print(f"Nombre de lignes : {len(df)}")

connection = psycopg2.connect(
    host="localhost",
    port=5433,
    database="taxi_db",
    user="taxi_user",
    password="taxi_password"
)

cursor = connection.cursor()

cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS taxi_trips (
        id SERIAL PRIMARY KEY,
        tpep_pickup_datetime TIMESTAMP,
        pulocationid INTEGER,
        pickup_zone TEXT,
        pickup_borough TEXT,
        trip_distance DOUBLE PRECISION,
        fare_amount DOUBLE PRECISION,
        passenger_count INTEGER
    )
""")

cursor.execute("TRUNCATE TABLE taxi_trips")

rows = []

for row in df.itertuples(index=False):
    rows.append((
        row.tpep_pickup_datetime.to_pydatetime(),
        int(row.PULocationID),
        row.pickup_zone,
        row.pickup_borough,
        float(row.trip_distance),
        float(row.fare_amount),
        int(row.passenger_count)
    ))

insert_query = """
    INSERT INTO taxi_trips (
        tpep_pickup_datetime,
        pulocationid,
        pickup_zone,
        pickup_borough,
        trip_distance,
        fare_amount,
        passenger_count
    ) VALUES %s
"""

execute_values(
    cursor,
    insert_query,
    rows,
    page_size=1000
)

connection.commit()

cursor.close()
connection.close()

print("Connexion à PostGIS réussie.")
print("Données enregistrées dans la table taxi_trips.")