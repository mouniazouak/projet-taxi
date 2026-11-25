# Guide de démarrage - Binôme 1

Ce fichier explique comment démarrer et vérifier la pipeline.

## 1. Ouvrir le projet

```powershell
cd "C:\Users\DELL\Documents\projet_taxi_binome1"
```

Si PowerShell bloque Activate.ps1, utilisez directement le Python du projet :

```powershell
& ".\.venv\Scripts\python.exe" script.py
```

## 2. Démarrer Docker

```powershell
docker start kafka mongodb-taxi postgis-taxi
docker ps
```

Ports : Kafka `9092`, MongoDB `27017`, PostGIS `5433`.

## 3. Préparer les données

```powershell
& ".\.venv\Scripts\python.exe" processing\read_zones.py
& ".\.venv\Scripts\python.exe" processing\test_sedona.py
& ".\.venv\Scripts\python.exe" processing\sedona_processing.py
```

Le résultat est créé dans `data/processed_trips.parquet`.

## 4. Charger les stockages

```powershell
& ".\.venv\Scripts\python.exe" storage\load_postgis.py
& ".\.venv\Scripts\python.exe" storage\load_mongodb.py
```

## 5. Simuler le temps réel avec Kafka

Dans le terminal 1, lancer le consumer MongoDB :

```powershell
$env:KAFKA_GROUP_ID="binome1-mongodb-test"
& ".\.venv\Scripts\python.exe" consumer\consumer_mongodb.py
```

Dans le terminal 2, lancer le producer :

```powershell
& ".\.venv\Scripts\python.exe" producer\producer.py
```

Le producer envoie 100 courses dans `taxi-trips-valid` et le consumer les enregistre dans MongoDB.

## 6. Vérifier MongoDB

```powershell
& ".\.venv\Scripts\python.exe" -c "from pymongo import MongoClient; c=MongoClient('mongodb://127.0.0.1:27017/'); print(c['taxi_db']['taxi_trips'].count_documents({}))"
```

Pour compter uniquement les courses Kafka :

```powershell
& ".\.venv\Scripts\python.exe" -c "from pymongo import MongoClient; c=MongoClient('mongodb://127.0.0.1:27017/'); col=c['taxi_db']['taxi_trips']; print(sum(1 for d in col.find({}, {'trip_id':1}) if d.get('trip_id','').startswith('TLC_')))"
```

Résultat attendu : 100 courses Kafka et environ 10 100 documents au total.

## 7. Générer la carte

```powershell
& ".\.venv\Scripts\python.exe" visualization\kepler_map.py
start visualization\carte_taxi.html
```

## 8. Arrêter le consumer

Dans son terminal : `Ctrl + C`.

## Architecture

```text
NYC TLC -> Python Producer -> Kafka -> Consumer -> MongoDB
                                      |
                                      +-> Spark/Sedona -> PostGIS -> Kepler.gl
```
