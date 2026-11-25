import pandas as pd
import geopandas as gpd
from keplergl import KeplerGl

print("Préparation de la carte...")

trips = pd.read_parquet("data/processed_trips.parquet")

zones = gpd.read_file(
    "data/taxi_zones/taxi_zones/taxi_zones.shp"
)

# Calcul des centres des zones
zones = zones.to_crs("EPSG:2263")
zones["geometry"] = zones.geometry.centroid
zones = zones.to_crs("EPSG:4326")

zones["pickup_longitude"] = zones.geometry.x
zones["pickup_latitude"] = zones.geometry.y

zones = zones[
    ["LocationID", "pickup_longitude", "pickup_latitude"]
]

# Association entre les courses et les zones
trips = trips.merge(
    zones,
    left_on="PULocationID",
    right_on="LocationID",
    how="left"
)

trips = trips.dropna(
    subset=["pickup_longitude", "pickup_latitude"]
)

carte = KeplerGl(
    height=700,
    data={"courses_taxi": trips}
)

carte.save_to_html(
    file_name="visualization/carte_taxi.html"
)

print("Carte créée avec succès.")
print("Fichier : visualization/carte_taxi.html")