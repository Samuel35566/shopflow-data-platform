"""Étape 2.6 — Zone SILVER des logs web.

  Lit   : lake/raw/web_logs/     (une ligne par page vue, horodatage en texte « +0100 »)
  Écrit : lake/silver/web_logs/  (Delta : vraie date en UTC, robots signalés, type de page)

Les robots ne sont PAS supprimés : ce sont de vraies visites, simplement pas humaines.
On les signale (colonne est_robot) ; les analyses de la phase 3 les écarteront.

Lancement :  make silver-logs
"""

from pathlib import Path

from pyspark.sql import functions as F

from session import creer_session

RACINE = Path(__file__).resolve().parents[1]
SOURCE_RAW = RACINE / "lake" / "raw" / "web_logs"
CIBLE_SILVER = RACINE / "lake" / "silver" / "web_logs"

# Format de l'horodatage des logs : 01/Jan/2026:11:34:09 +0100
FORMAT_HORODATAGE = "dd/MMM/yyyy:HH:mm:ss Z"
MODELE_ROBOT = r"bot|crawler|spider"

spark = creer_session("silver_logs")

brut = spark.read.parquet(str(SOURCE_RAW)).drop("date")
print(f"Lu dans raw : {brut.count()} lignes")

silver = (
    brut
    # Le texte devient une vraie date. Le « +0100 » est pris en compte, et la session
    # étant réglée en UTC, l'heure est convertie : 11:34:09 +0100 → 10:34:09 UTC.
    .withColumn("visite_le", F.try_to_timestamp("horodatage", F.lit(FORMAT_HORODATAGE)))
    .withColumn("est_robot", F.lower("user_agent").rlike(MODELE_ROBOT))
    # Le type de page : le premier mot de l'adresse ("/produit/42" → "produit", "/" → "accueil")
    .withColumn("type_page", F.regexp_extract("url", r"^/([a-z]+)", 1))
    .withColumn("type_page", F.when(F.col("type_page") == "", "accueil").otherwise(F.col("type_page")))
    # Le numéro du produit consulté, quand c'est une page produit ("/produit/42" → 42)
    .withColumn("product_id", F.regexp_extract("url", r"^/produit/(\d+)", 1).try_cast("int"))
    .withColumn("_silver_at", F.current_timestamp())
)

silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(CIBLE_SILVER))

relu = spark.read.format("delta").load(str(CIBLE_SILVER))
print(f"Écrit dans silver : {relu.count()} lignes")
print(f"Dates non reconnues : {relu.filter(F.col('visite_le').isNull()).count()}")

print("\nRobots ou humains :")
relu.groupBy("est_robot").count().show()

print("Pages vues par des HUMAINS, par type de page :")
relu.filter(~F.col("est_robot")).groupBy("type_page").count().orderBy(F.desc("count")).show()

print("Exemple : l'horodatage d'origine et la date convertie en UTC")
relu.select("horodatage", "visite_le").show(3, truncate=False)

spark.stop()
