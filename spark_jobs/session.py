"""Création de la session Spark, configurée pour Delta Lake.

Tous les scripts de la phase 2 commencent par :
    from session import creer_session
    spark = creer_session("nom_du_job")
"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def creer_session(nom: str) -> SparkSession:
    """Démarre Spark en local, avec Delta Lake activé."""
    builder = (
        SparkSession.builder.appName(nom)
        .master("local[*]")  # en local, en utilisant tous les cœurs du processeur
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")  # toutes les dates en UTC
        .config("spark.driver.memory", "2g")  # mémoire maximale donnée à Spark
    )
    # Ajoute automatiquement les fichiers Java de Delta Lake (téléchargés au 1er lancement)
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # moins de messages techniques à l'écran
    return spark
