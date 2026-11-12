"""Étape 3.2 — Zone SILVER des lignes de commande.

  Lit   : lake/raw/order_items/                            (un snapshot complet par jour)
          lake/silver/orders/ + lake/quarantaine/orders/   (pour vérifier que la commande existe)
          lake/silver/products/                            (pour repérer les produits hors catalogue)
  Écrit : lake/silver/order_items/       (Delta : une ligne par article vendu, avec son montant)
          lake/quarantaine/order_items/  (Delta : lignes refusées, avec la raison)

Un produit absent du catalogue n'est PAS une raison de quarantaine : la vente a bien eu lieu.
On le signale seulement (colonne produit_au_catalogue) ; dbt le rattachera à un produit « inconnu ».

Prérequis : make silver-orders et make silver-products
Lancement : make silver-order-items
"""

from pathlib import Path

from pyspark.sql import functions as F

from session import creer_session

RACINE = Path(__file__).resolve().parents[1]
SOURCE_RAW = RACINE / "lake" / "raw" / "order_items"
SILVER_COMMANDES = RACINE / "lake" / "silver" / "orders"
QUARANTAINE_COMMANDES = RACINE / "lake" / "quarantaine" / "orders"
SILVER_PRODUITS = RACINE / "lake" / "silver" / "products"
CIBLE_SILVER = RACINE / "lake" / "silver" / "order_items"
CIBLE_QUARANTAINE = RACINE / "lake" / "quarantaine" / "order_items"

spark = creer_session("silver_lignes_commande")

# ── 1. LIRE le snapshot le plus récent ──────────────────────────────────────
brut = spark.read.parquet(str(SOURCE_RAW))
dernier_jour = brut.agg(F.max("date")).first()[0]
lignes = brut.filter(F.col("date") == dernier_jour).drop("date")
print(f"Snapshot utilisé : {dernier_jour} ({lignes.count()} lignes)")

# ── 2. NETTOYER et CALCULER le montant de chaque ligne ──────────────────────
nettoye = (
    lignes
    .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
    .withColumn("discount", F.coalesce(F.col("discount"), F.lit(0)).cast("decimal(4,2)"))  # pas de remise = 0
    # montant = quantité × prix unitaire × (1 − remise)   ex. 2 × 50.00 × (1 − 0.10) = 90.00
    .withColumn(
        "montant_ligne",
        (F.col("quantity") * F.col("unit_price") * (1 - F.col("discount"))).cast("decimal(12,2)"),
    )
)

# ── 3. LES COMMANDES ET LES PRODUITS CONNUS ─────────────────────────────────
commandes_connues = (
    spark.read.format("delta").load(str(SILVER_COMMANDES)).select("order_id")
    .unionByName(spark.read.format("delta").load(str(QUARANTAINE_COMMANDES)).select("order_id"))
    .distinct()
    .withColumn("commande_existe", F.lit(True))
)
produits_catalogue = (
    spark.read.format("delta").load(str(SILVER_PRODUITS)).select("product_id")
    .withColumn("produit_au_catalogue", F.lit(True))
)
avec_references = (
    nettoye
    .join(commandes_connues, on="order_id", how="left")
    .join(F.broadcast(produits_catalogue), on="product_id", how="left")
    .withColumn("produit_au_catalogue", F.coalesce(F.col("produit_au_catalogue"), F.lit(False)))
)

# ── 4. CONTRÔLER ────────────────────────────────────────────────────────────
avec_controle = avec_references.withColumn(
    "_raison_rejet",
    F.when(F.col("commande_existe").isNull(), "commande inconnue")
     .when(F.col("product_id").isNull(), "produit manquant")
     .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "quantité invalide")
     .when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "prix unitaire invalide")
     .when((F.col("discount") < 0) | (F.col("discount") >= 1), "remise invalide"),
)
silver = avec_controle.filter(F.col("_raison_rejet").isNull()).drop("_raison_rejet", "commande_existe")
quarantaine = avec_controle.filter(F.col("_raison_rejet").isNotNull()).drop("commande_existe")

# ── 5. ÉCRIRE ───────────────────────────────────────────────────────────────
silver = silver.withColumn("_silver_at", F.current_timestamp())
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(CIBLE_SILVER))
quarantaine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(CIBLE_QUARANTAINE))

# ── 6. CONTRÔLER le résultat ────────────────────────────────────────────────
relu = spark.read.format("delta").load(str(CIBLE_SILVER))
nb_lignes = relu.count()
nb_uniques = relu.select("order_id", "line_number").distinct().count()
print(f"Écrit dans silver    : {nb_lignes} lignes")
print(f"Écrit en quarantaine : {spark.read.format('delta').load(str(CIBLE_QUARANTAINE)).count()} lignes")
print("Contrôle des doublons : " + ("✅ aucun" if nb_lignes == nb_uniques else "❌ IL RESTE DES DOUBLONS"))

print("\nRaisons de mise en quarantaine :")
spark.read.format("delta").load(str(CIBLE_QUARANTAINE)).groupBy("_raison_rejet").count().show(truncate=False)

print("Produits présents au catalogue ou non :")
relu.groupBy("produit_au_catalogue").count().show()

spark.stop()