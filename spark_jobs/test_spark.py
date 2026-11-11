"""Premier test : Spark démarre, écrit et relit une table Delta, puis lit le data lake."""

from pathlib import Path

from session import creer_session

RACINE = Path(__file__).resolve().parents[1]

spark = creer_session("test_spark")
print(f"\n✅ Spark {spark.version} a démarré\n")

# 1. Un petit tableau (un « DataFrame ») créé à la main
clients = spark.createDataFrame([(1, "Léa", "France"), (2, "Hugo", "Belgique")], ["id", "prenom", "pays"])
clients.show()

# 2. Écrire ce tableau au format Delta, puis le relire
chemin_test = str(RACINE / "lake" / "_tests" / "delta_test")
clients.write.format("delta").mode("overwrite").save(chemin_test)
print("✅ Table Delta écrite puis relue :")
spark.read.format("delta").load(chemin_test).show()

# 3. Lire tes vraies commandes du data lake (phase 1)
commandes = spark.read.parquet(str(RACINE / "lake" / "raw" / "orders"))
print(f"✅ {commandes.count()} commandes lues dans lake/raw/orders\n")

spark.stop()
