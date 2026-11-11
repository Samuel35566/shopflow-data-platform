"""Étape 2.7 — Delta Lake en action : historique, MERGE et voyage dans le temps.

On simule une correction du service client sur la table silver des commandes :
la commande 42 passe au statut « refunded ». Puis on remonte dans le temps
pour revoir la table AVANT la correction.

Prérequis : make silver-orders
Lancement : make delta-demo
"""

from pathlib import Path

from delta.tables import DeltaTable

from session import creer_session

RACINE = Path(__file__).resolve().parents[1]
CHEMIN = str(RACINE / "lake" / "silver" / "orders")


def titre(texte: str) -> None:
    print(f"\n{'═' * 70}\n  {texte}\n{'═' * 70}")


spark = creer_session("delta_demo")
table = DeltaTable.forPath(spark, CHEMIN)

titre("1. L'historique de la table : chaque écriture est une VERSION")
table.history().select("version", "timestamp", "operation").orderBy("version").show(truncate=False)
version_avant = table.history(1).select("version").first()[0]

titre("2. La commande 42 AVANT la correction")
table.toDF().filter("order_id = 42").select("order_id", "status", "total_amount").show()

titre("3. MERGE : appliquer une correction sans réécrire toute la table")
corrections = spark.createDataFrame([(42, "refunded")], ["order_id", "nouveau_status"])
(
    table.alias("cible")
    .merge(corrections.alias("source"), "cible.order_id = source.order_id")
    .whenMatchedUpdate(set={"status": "source.nouveau_status"})  # la commande existe : on la met à jour
    .execute()
)
print("Correction appliquée.")
table.toDF().filter("order_id = 42").select("order_id", "status", "total_amount").show()

titre("4. L'historique s'est allongé d'une version MERGE")
table.history().select("version", "timestamp", "operation", "operationMetrics.numTargetRowsUpdated") \
    .orderBy("version").show(truncate=False)

titre(f"5. VOYAGE DANS LE TEMPS : la table telle qu'elle était à la version {version_avant}")
ancienne = spark.read.format("delta").option("versionAsOf", version_avant).load(CHEMIN)
ancienne.filter("order_id = 42").select("order_id", "status", "total_amount").show()

titre("6. Comparer deux versions : combien de lignes ont changé ?")
actuelle = spark.read.format("delta").load(CHEMIN)
nb_differences = actuelle.select("order_id", "status").exceptAll(ancienne.select("order_id", "status")).count()
print(f"{nb_differences} ligne(s) différente(s) entre la version {version_avant} et aujourd'hui")

print("\n💡 Pour annuler la correction : make silver-orders recalcule la table depuis raw,")
print("   ou bien, en SQL : RESTORE TABLE delta.`lake/silver/orders` TO VERSION AS OF", version_avant)

spark.stop()
