import geopandas as gpd

file_path = "data/taxi_zones/taxi_zones/taxi_zones.shp"

zones = gpd.read_file(file_path)

print("Nombre de zones :", len(zones))
print("Colonnes :", zones.columns.tolist())
print("Système de coordonnées :", zones.crs)
print(zones.head())