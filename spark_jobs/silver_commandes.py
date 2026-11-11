"""Étape 2.3 — Zone SILVER des commandes : nettoyer les données brutes avec Spark.

  Lit       : lake/raw/orders/            (Parquet, brut, avec doublons)
  Écrit     : lake/silver/orders/         (Delta, une ligne par commande, propre)
              lake/quarantaine/orders/    (Delta, les commandes refusées, avec la raison)

La table silver est entièrement RECALCULÉE à chaque exécution à partir de raw :
on peut donc la supprimer et la reconstruire à tout moment (idempotence).

Lancement :  make silver-orders
"""

from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from session import creer_session

RACINE = Path(__file__).resolve().parents[1]
SOURCE_RAW = RACINE / "lake" / "raw" / "orders"
CIBLE_SILVER = RACINE / "lake" / "silver" / "orders"
CIBLE_QUARANTAINE = RACINE / "lake" / "quarantaine" / "orders"

STATUTS_CONNUS = ["paid", "shipped", "delivered", "cancelled", "refunded"]

spark = creer_session("silver_commandes")

# ── 1. LIRE la zone raw ──────────────────────────────────────────────────────
brut = spark.read.parquet(str(SOURCE_RAW)).drop("date")  # "date" = partition du lac, inutile ici
print(f"Lu dans raw : {brut.count()} lignes")

# ── 2. DÉDOUBLONNER : garder la version la plus récente de chaque commande ──
# On numérote les versions de chaque commande, de la plus récente (1) à la plus ancienne.
fenetre = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc(), F.col("_ingested_at").desc())
derniere_version = (
    brut
    .withColumn("_version", F.row_number().over(fenetre))
    .filter(F.col("_version") == 1)
    .drop("_version")
)

# ── 3. NETTOYER les valeurs ─────────────────────────────────────────────────
nettoye = (
    derniere_version
    .withColumn("status", F.lower(F.trim("status")))        # "PAID" -> "paid"
    .withColumn("currency", F.upper(F.trim("currency")))    # " eur" -> "EUR"
)

# ── 4. VÉRIFIER chaque commande : quelle règle métier est violée ? ─────────
# F.when(...).when(...) fonctionne comme un « si… sinon si… » : la 1re règle violée l'emporte.
avec_controle = nettoye.withColumn(
    "_raison_rejet",
    F.when(F.col("customer_id").isNull(), "client manquant")
     .when(F.col("total_amount").isNull(), "montant manquant")
     .when(F.col("total_amount") < 0, "montant négatif")
     .when(~F.col("status").isin(STATUTS_CONNUS), "statut inconnu"),
)  # si aucune règle n'est violée, _raison_rejet reste vide (null)

silver = avec_controle.filter(F.col("_raison_rejet").isNull()).drop("_raison_rejet")
quarantaine = avec_controle.filter(F.col("_raison_rejet").isNotNull())

# ── 5. ÉCRIRE au format Delta (on remplace entièrement l'ancienne version) ──
silver = silver.withColumn("_silver_at", F.current_timestamp())
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(CIBLE_SILVER))
quarantaine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(CIBLE_QUARANTAINE))

# ── 6. CONTRÔLER le résultat en relisant ce qui a été écrit ─────────────────
relu = spark.read.format("delta").load(str(CIBLE_SILVER))
nb_lignes = relu.count()
nb_commandes = relu.select("order_id").distinct().count()
print(f"Écrit dans silver      : {nb_lignes} commandes")
print(f"Écrit en quarantaine   : {spark.read.format('delta').load(str(CIBLE_QUARANTAINE)).count()} commandes")
print("Contrôle des doublons  : " + ("✅ aucun" if nb_lignes == nb_commandes else "❌ IL RESTE DES DOUBLONS"))

print("\nRépartition par statut dans silver :")
relu.groupBy("status").count().orderBy(F.desc("count")).show()

print("Raisons de mise en quarantaine :")
spark.read.format("delta").load(str(CIBLE_QUARANTAINE)).groupBy("_raison_rejet").count().show()

spark.stop()