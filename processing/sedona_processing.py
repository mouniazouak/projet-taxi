import geopandas as gpd

from pyspark.sql.functions import col, expr
from sedona.spark import SedonaContext


TRIPS_FILE = "data/yellow_tripdata_2024-01.parquet"
ZONES_FILE = "data/taxi_zones/taxi_zones/taxi_zones.shp"
OUTPUT_FILE = "data/processed_trips"


print("Démarrage de Sedona...")

config = (
    SedonaContext.builder()
    .master("local[2]")
    .appName("TaxiSedonaProcessing")
    .config(
        "spark.jars.packages",
        "org.apache.sedona:sedona-spark-shaded-4.1_2.13:1.9.1"
    )
    .getOrCreate()
)

sedona = SedonaContext.create(config)

print("Lecture des trajets...")

trips = (
    sedona.read.parquet(TRIPS_FILE)
    .select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "fare_amount",
        "passenger_count"
    )
    .filter(col("tpep_pickup_datetime") >= "2024-01-01")
    .filter(col("tpep_pickup_datetime") < "2024-02-01")
    .filter(col("trip_distance") > 0)
    .filter(col("fare_amount") >= 0)
    .filter(col("passenger_count").between(1, 8))
    .limit(10000)
)

print("Lecture des zones géographiques...")

zones = gpd.read_file(ZONES_FILE)

# Conversion du système EPSG:2263 vers EPSG:4326
zones = zones.to_crs("EPSG:4326")

zones["geometry_wkt"] = zones.geometry.to_wkt()

zones_spark = sedona.createDataFrame(
    zones[
        ["LocationID", "zone", "borough", "geometry_wkt"]
    ]
)

zones_spark = (
    zones_spark
    .withColumn(
        "geometry",
        expr("ST_GeomFromWKT(geometry_wkt)")
    )
    .drop("geometry_wkt")
)

pickup_zones = zones_spark.select(
    col("LocationID").alias("PULocationID"),
    col("zone").alias("pickup_zone"),
    col("borough").alias("pickup_borough"),
    col("geometry").alias("pickup_geometry")
)

result = trips.join(
    pickup_zones,
    on="PULocationID",
    how="left"
)

print("Résultat du traitement :")
result.select(
    "tpep_pickup_datetime",
    "PULocationID",
    "pickup_zone",
    "pickup_borough",
    "trip_distance",
    "fare_amount"
).show(10, truncate=False)

print("Nombre de trajets par arrondissement :")

result.groupBy("pickup_borough").count().show()

output_columns = [
    "tpep_pickup_datetime",
    "PULocationID",
    "pickup_zone",
    "pickup_borough",
    "trip_distance",
    "fare_amount",
    "passenger_count"
]

output_df = result.select(output_columns).toPandas()

output_df.to_parquet(
    "data/processed_trips.parquet",
    index=False
)

print("Résultat enregistré dans : data/processed_trips.parquet")

print("Traitement Sedona terminé.")
print(f"Résultat enregistré dans : {OUTPUT_FILE}")

sedona.stop()