# Pipeline géospatial des courses de taxis

Projet du Binôme 1 basé sur les données NYC TLC Yellow Taxi Trip Records.

## Architecture

```text
NYC TLC Parquet -> Python Producer -> Kafka -> Consumer -> MongoDB
                                      |
                                      +-> Spark + Apache Sedona -> PostGIS -> Kepler.gl
```

Le fichier NYC TLC est historique. Kafka permet de simuler un flux temps réel en envoyant les courses progressivement.

## Installation

```cmd
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
```

## Services Docker

```cmd
docker run -d --name kafka -p 9092:9092 apache/kafka:4.4.0
docker run -d --name mongodb-taxi -p 27017:27017 mongo:8.0
docker run -d --name postgis-taxi -e POSTGRES_USER=taxi_user -e POSTGRES_PASSWORD=taxi_password -e POSTGRES_DB=taxi_db -p 5433:5432 postgis/postgis:16-3.4
```

## Exécution

```cmd
python producer\\producer.py
python consumer\\consumer.py
python consumer\\consumer_mongodb.py
python processing\\sedona_processing.py
python storage\\load_postgis.py
python storage\\load_mongodb.py
python visualization\\kepler_map.py
```

Résultats vérifiés : 10 000 courses traitées, stockage PostGIS et MongoDB chargés, et carte Kepler.gl générée.
