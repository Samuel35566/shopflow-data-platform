"""Étape 2.5 — Zone SILVER des paiements.

  Lit   : lake/raw/payments/            (Parquet, montants en centimes, dates Unix, 4 devises)
          lake/silver/orders/ + lake/quarantaine/orders/   (pour vérifier que la commande existe)
  Écrit : lake/silver/payments/         (Delta : montants en devise et en euros, vraies dates)
          lake/quarantaine/payments/    (Delta : paiements refusés, avec la raison)

Prérequis : make silver-orders (on a besoin des commandes connues)
Lancement : make silver-payments
"""

from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

from session import creer_session

RACINE = Path(__file__).resolve().parents[1]
SOURCE_RAW = RACINE / "lake" / "raw" / "payments"
SILVER_COMMANDES = RACINE / "lake" / "silver" / "orders"
QUARANTAINE_COMMANDES = RACINE / "lake" / "quarantaine" / "orders"
CIBLE_SILVER = RACINE / "lake" / "silver" / "payments"
CIBLE_QUARANTAINE = RACINE / "lake" / "quarantaine" / "payments"

spark = creer_session("silver_paiements")

# ── 1. LIRE ─────────────────────────────────────────────────────────────────
brut = spark.read.parquet(str(SOURCE_RAW)).drop("date")
print(f"Lu dans raw : {brut.count()} paiements")

# Un même paiement pourrait avoir été collecté deux fois (fichier retraité) : on garde le plus récent
fenetre = Window.partitionBy("payment_id").orderBy(F.col("_ingested_at").desc())
paiements = brut.withColumn("_n", F.row_number().over(fenetre)).filter(F.col("_n") == 1).drop("_n")

# ── 2. LA TABLE DES TAUX DE CHANGE (combien d'unités de la devise pour 1 euro) ─
taux_change = spark.createDataFrame(
    [("EUR", 1.00), ("USD", 1.08), ("CHF", 0.95), ("GBP", 0.86)],
    ["currency", "taux_pour_1_eur"],
)

# ── 3. NETTOYER ─────────────────────────────────────────────────────────────
nettoye = (
    paiements
    .withColumn("currency", F.upper(F.trim("currency")))                             # "usd" → "USD"
    .withColumn("montant_devise", (F.col("amount") / 100).cast("decimal(12,2)"))     # 106693 → 1066.93
    .withColumn("paye_le", F.timestamp_seconds("created"))                           # 1767700248 → 2026-01-06 11:50:48
    .withColumn("order_id", F.col("order_id").try_cast("long"))
    .withColumn("status", F.lower(F.trim("status")))
    .join(F.broadcast(taux_change), on="currency", how="left")
    .withColumn("montant_eur", (F.col("montant_devise") / F.col("taux_pour_1_eur")).cast("decimal(12,2)"))
    .drop("amount", "created")
)

# ── 4. LES COMMANDES CONNUES (propres ET en quarantaine : elles existent toutes) ─
commandes_connues = (
    spark.read.format("delta").load(str(SILVER_COMMANDES)).select("order_id")
    .unionByName(spark.read.format("delta").load(str(QUARANTAINE_COMMANDES)).select("order_id"))
    .distinct()
    .withColumn("commande_existe", F.lit(True))
)
avec_commande = nettoye.join(commandes_connues, on="order_id", how="left")

# ── 5. CONTRÔLER ────────────────────────────────────────────────────────────
avec_controle = avec_commande.withColumn(
    "_raison_rejet",
    F.when(F.col("taux_pour_1_eur").isNull(), "devise inconnue")
     .when(F.col("montant_devise") <= 0, "montant négatif ou nul")
     .when(F.col("paye_le") > F.current_timestamp(), "date dans le futur")
     .when(F.col("commande_existe").isNull(), "commande inconnue (paiement orphelin)"),
)
silver = avec_controle.filter(F.col("_raison_rejet").isNull()).drop("_raison_rejet", "commande_existe")
quarantaine = avec_controle.filter(F.col("_raison_rejet").isNotNull()).drop("commande_existe")

# ── 6. ÉCRIRE ───────────────────────────────────────────────────────────────
silver = silver.withColumn("_silver_at", F.current_timestamp())
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(CIBLE_SILVER))
quarantaine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(CIBLE_QUARANTAINE))

# ── 7. CONTRÔLER le résultat ────────────────────────────────────────────────
relu = spark.read.format("delta").load(str(CIBLE_SILVER))
print(f"Écrit dans silver    : {relu.count()} paiements")
print(f"Écrit en quarantaine : {spark.read.format('delta').load(str(CIBLE_QUARANTAINE)).count()} paiements")

print("\nRaisons de mise en quarantaine :")
spark.read.format("delta").load(str(CIBLE_QUARANTAINE)).groupBy("_raison_rejet").count().show(truncate=False)

print("Paiements par statut (les « failed » restent en silver : ce sont de vraies informations) :")
relu.groupBy("status").count().show()

print("Encaissé en euros (paiements réussis uniquement) :")
relu.filter(F.col("status") == "succeeded").agg(F.sum("montant_eur").alias("total_eur")).show()

spark.stop()
