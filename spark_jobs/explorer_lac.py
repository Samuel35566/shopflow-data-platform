"""Étape 2.2 — Premiers pas avec Spark : explorer le data lake (zone raw).

Ce script ne modifie rien : il lit les données de la phase 1 et affiche
des résultats, pour apprendre les gestes de base de Spark.

Lancement :  make explore
"""

from pathlib import Path

from pyspark.sql import functions as F

from session import creer_session

RACINE = Path(__file__).resolve().parents[1]
RAW = RACINE / "lake" / "raw"


def titre(texte: str) -> None:
    """Affiche un titre bien visible entre deux exemples."""
    print(f"\n{'═' * 70}\n  {texte}\n{'═' * 70}")


spark = creer_session("explorer_lac")

# ── LIRE : chaque dossier du data lake devient un « DataFrame » (un tableau Spark)
commandes = spark.read.parquet(str(RAW / "orders"))
produits = spark.read.parquet(str(RAW / "products"))
logs = spark.read.parquet(str(RAW / "web_logs"))

titre("1. La structure de la table des commandes (printSchema)")
commandes.printSchema()

titre("2. Les 5 premières lignes, quelques colonnes seulement (select + show)")
commandes.select("order_id", "customer_id", "order_date", "status", "total_amount", "currency").show(5)

titre("3. Compter les lignes et les commandes distinctes (count, distinct)")
nb_lignes = commandes.count()
nb_commandes = commandes.select("order_id").distinct().count()
print(f"{nb_lignes} lignes, mais seulement {nb_commandes} commandes distinctes")

titre("4. Filtrer : les montants négatifs (filter)")
negatifs = commandes.filter(F.col("total_amount") < 0)
print(f"{negatifs.count()} commandes avec un montant négatif, par exemple :")
negatifs.select("order_id", "status", "total_amount").show(3)

titre("5. Grouper : le nombre de commandes par statut (groupBy + count)")
commandes.groupBy("status").count().orderBy(F.desc("count")).show()

titre("6. Les doublons : les commandes présentes plusieurs fois")
doublons = commandes.groupBy("order_id").count().filter(F.col("count") > 1)
print(f"{doublons.count()} commandes apparaissent plusieurs fois, par exemple :")
doublons.orderBy(F.desc("count"), "order_id").show(5)

titre("7. Créer une colonne : le statut en minuscules (withColumn)")
(commandes
    .withColumn("status_propre", F.lower("status"))
    .groupBy("status_propre").count()
    .orderBy(F.desc("count"))
    .show())

titre("8. Les prix des produits : combien sont écrits en texte avec « € » ?")
produits.groupBy(F.col("price").contains("€").alias("prix_avec_euro")).count().show()

titre("9. Écrire du SQL directement dans Spark : robots ou humains ?")
logs.createOrReplaceTempView("web_logs")  # rend le DataFrame interrogeable en SQL
spark.sql("""
    SELECT
        CASE WHEN lower(user_agent) LIKE '%bot%' THEN 'robot' ELSE 'humain' END AS visiteur,
        count(*) AS nb_visites,
        round(100 * count(*) / sum(count(*)) OVER (), 1) AS pourcentage
    FROM web_logs
    GROUP BY 1
    ORDER BY nb_visites DESC
""").show()

spark.stop()