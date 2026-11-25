from sedona.spark import SedonaContext

print("Démarrage de Sedona...")

try:
    config = (
        SedonaContext.builder()
        .master("local[2]")
        .appName("TaxiSedona")
        .config(
            "spark.jars.packages",
            "org.apache.sedona:sedona-spark-shaded-4.1_2.13:1.9.1"
        )
        .getOrCreate()
    )

    sedona = SedonaContext.create(config)

    print("Sedona Spark fonctionne correctement")

    sedona.stop()

except Exception as erreur:
    print("ERREUR :", erreur)