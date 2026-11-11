"""Étape 2.4 — Zone SILVER des produits.

  Lit   : lake/raw/products/          (un « snapshot » complet du catalogue par jour)
  Écrit : lake/silver/products/       (Delta : prix en nombre, catégories harmonisées)
          lake/quarantaine/products/  (Delta : produits refusés, avec la raison)

Lancement :  make silver-products
"""

from pathlib import Path

from pyspark.sql import functions as F

from session import creer_session

RACINE = Path(__file__).resolve().parents[1]
SOURCE_RAW = RACINE / "lake" / "raw" / "products"
CIBLE_SILVER = RACINE / "lake" / "silver" / "products"
CIBLE_QUARANTAINE = RACINE / "lake" / "quarantaine" / "products"

spark = creer_session("silver_produits")

# ── 1. LIRE le snapshot le plus récent ──────────────────────────────────────
# La zone raw contient un catalogue complet PAR JOUR d'extraction : on ne garde que le dernier.
brut = spark.read.parquet(str(SOURCE_RAW))
dernier_jour = brut.agg(F.max("date")).first()[0]
catalogue = brut.filter(F.col("date") == dernier_jour).drop("date")
print(f"Snapshot utilisé : {dernier_jour} ({catalogue.count()} produits)")

# ── 2. LA TABLE DE CORRESPONDANCE des catégories ────────────────────────────
# Toutes les façons d'écrire une catégorie (après minuscules) → le nom officiel.
correspondance_categories = spark.createDataFrame(
    [
        ("running", "Running"),
        ("vélo", "Vélo"),
        ("velo", "Vélo"),
        ("fitness", "Fitness"),
        ("natation", "Natation"),
        ("randonnée", "Randonnée"),
        ("rando", "Randonnée"),
    ],
    ["categorie_brute", "categorie"],
)

# ── 3. NETTOYER ─────────────────────────────────────────────────────────────
nettoye = (
    catalogue
    .withColumnRenamed("id", "product_id")
    .withColumnRenamed("name", "nom")
    .withColumnRenamed("brand", "marque")
    # Prix : "212,65 €" → "212,65" → "212.65" → 212.65
    .withColumn("prix_texte", F.regexp_replace("price", r"[^0-9,.]", ""))   # garde chiffres, virgule, point
    .withColumn("prix_texte", F.regexp_replace("prix_texte", ",", "."))     # virgule française → point
    .withColumn("prix", F.col("prix_texte").try_cast("decimal(10,2)"))      # null si illisible (pas d'erreur)
    .withColumn("updated_at", F.col("updated_at").try_cast("timestamp"))
    # Catégorie : on met en minuscules sans espaces, puis on cherche le nom officiel
    .withColumn("categorie_brute", F.lower(F.trim("category")))
    .join(F.broadcast(correspondance_categories), on="categorie_brute", how="left")
    .drop("price", "prix_texte", "category")
)

# ── 4. CONTRÔLER ────────────────────────────────────────────────────────────
avec_controle = nettoye.withColumn(
    "_raison_rejet",
    F.when(F.col("prix").isNull(), "prix illisible")
     .when(F.col("prix") <= 0, "prix négatif ou nul")
     .when(F.col("categorie").isNull(), "catégorie inconnue"),
)
silver = avec_controle.filter(F.col("_raison_rejet").isNull()).drop("_raison_rejet", "categorie_brute")
quarantaine = avec_controle.filter(F.col("_raison_rejet").isNotNull())

# ── 5. ÉCRIRE ───────────────────────────────────────────────────────────────
silver = silver.withColumn("_silver_at", F.current_timestamp())
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(CIBLE_SILVER))
quarantaine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(CIBLE_QUARANTAINE))

# ── 6. CONTRÔLER le résultat ────────────────────────────────────────────────
relu = spark.read.format("delta").load(str(CIBLE_SILVER))
print(f"Écrit dans silver    : {relu.count()} produits")
print(f"Écrit en quarantaine : {spark.read.format('delta').load(str(CIBLE_QUARANTAINE)).count()} produits")

print("\nProduits par catégorie (après harmonisation) :")
relu.groupBy("categorie").count().orderBy(F.desc("count")).show()

print("Quelques prix convertis :")
relu.select("product_id", "nom", "categorie", "prix").orderBy("product_id").show(5, truncate=False)

spark.stop()
