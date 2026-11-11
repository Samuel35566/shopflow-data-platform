"""Étape 2.4 — Zone SILVER des clients.

  Lit   : lake/raw/clients/          (un « snapshot » complet de l'export CRM par jour)
  Écrit : lake/silver/clients/       (Delta : emails vérifiés, pays et segments harmonisés, vraies dates)
          lake/quarantaine/clients/  (Delta : clients inutilisables, avec la raison)

Lancement :  make silver-clients
"""

from itertools import chain
from pathlib import Path

from pyspark.sql import functions as F

from session import creer_session

RACINE = Path(__file__).resolve().parents[1]
SOURCE_RAW = RACINE / "lake" / "raw" / "clients"
CIBLE_SILVER = RACINE / "lake" / "silver" / "clients"
CIBLE_QUARANTAINE = RACINE / "lake" / "quarantaine" / "clients"

# Un email valide : du texte sans @ ni espace, un @, du texte, un point, une extension
MODELE_EMAIL = r"^[^@\s]+@[^@\s]+\.[a-z]+$"

# Toutes les façons d'écrire un pays (après minuscules) → le nom officiel
PAYS = {
    "france": "France",
    "fr": "France",
    "belgique": "Belgique",
    "suisse": "Suisse",
    "espagne": "Espagne",
    "allemagne": "Allemagne",
}
# Transforme le dictionnaire Python en « carte » utilisable par Spark

spark = creer_session("silver_clients")

CARTE_PAYS = F.create_map(*[F.lit(x) for x in chain(*PAYS.items())])

# ── 1. LIRE le snapshot le plus récent ──────────────────────────────────────
brut = spark.read.parquet(str(SOURCE_RAW))
dernier_jour = brut.agg(F.max("date")).first()[0]
clients = brut.filter(F.col("date") == dernier_jour).drop("date")
print(f"Snapshot utilisé : {dernier_jour} ({clients.count()} clients)")

# ── 2. NETTOYER ─────────────────────────────────────────────────────────────
date_brute = F.trim("date_inscription")
nettoye = (
    clients
    .withColumn("client_id", F.col("client_id").try_cast("int"))
    .withColumn("email", F.lower(F.trim("email")))
    # Un email invalide n'empêche pas d'utiliser le client : on garde le client,
    # on vide seulement l'email douteux, et on le signale dans une colonne.
    .withColumn("email_valide", F.col("email").rlike(MODELE_EMAIL))
    .withColumn("email", F.when(F.col("email_valide"), F.col("email")))
    .withColumn("pays", F.try_element_at(CARTE_PAYS, F.lower(F.trim("pays"))))  # inconnu ou vide → null
    .withColumn("segment", F.upper(F.trim("segment")))
    .withColumn("segment", F.when(F.col("segment") != "", F.col("segment")))  # "" → null
    # Deux formats de date dans le fichier : on reconnaît chacun avec une expression régulière
    .withColumn(
        "date_inscription",
        F.when(date_brute.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.to_date(date_brute, "yyyy-MM-dd"))
         .when(date_brute.rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(date_brute, "dd/MM/yyyy")),
    )
)

# ── 3. CONTRÔLER : seuls les clients vraiment inutilisables vont en quarantaine
avec_controle = nettoye.withColumn(
    "_raison_rejet",
    F.when(F.col("client_id").isNull(), "identifiant manquant")
     .when(F.col("nom").isNull() | (F.trim("nom") == ""), "nom manquant"),
)
silver = avec_controle.filter(F.col("_raison_rejet").isNull()).drop("_raison_rejet")
quarantaine = avec_controle.filter(F.col("_raison_rejet").isNotNull())

# ── 4. ÉCRIRE ───────────────────────────────────────────────────────────────
silver = silver.withColumn("_silver_at", F.current_timestamp())
silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(CIBLE_SILVER))
quarantaine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(str(CIBLE_QUARANTAINE))

# ── 5. CONTRÔLER le résultat ────────────────────────────────────────────────
relu = spark.read.format("delta").load(str(CIBLE_SILVER))
print(f"Écrit dans silver    : {relu.count()} clients")
print(f"Écrit en quarantaine : {spark.read.format('delta').load(str(CIBLE_QUARANTAINE)).count()} clients")

print("\nEmails valides ou non :")
relu.groupBy("email_valide").count().show()
print("Clients par pays (après harmonisation) :")
relu.groupBy("pays").count().orderBy(F.desc("count")).show()
print("Clients par segment :")
relu.groupBy("segment").count().orderBy(F.desc("count")).show()
print(f"Dates d'inscription non reconnues : {relu.filter(F.col('date_inscription').isNull()).count()}")

spark.stop()
