# Makefile du projet ShopFlow
# Tape `make` (ou `make help`) pour voir toutes les commandes disponibles.

PYTHON ?= python3
VENV   := .venv
BIN    := $(VENV)/bin
# Utilise le Python de l'environnement virtuel s'il existe, sinon celui du système
PY     := $(if $(wildcard $(BIN)/python),$(BIN)/python,$(PYTHON))

.DEFAULT_GOAL := help
.PHONY: help venv install data test lint clean

help: ## Affiche cette aide
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  make %-10s %s\n", $$1, $$2}'

venv: ## Crée l'environnement virtuel Python dans .venv/
	$(PYTHON) -m venv $(VENV)

install: venv ## Installe les dépendances de requirements.txt
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -r requirements.txt

data: ## Génère les 6 sources de données dans sources/
	$(PY) generer_donnees.py

test: ## Lance les tests automatiques
	$(PY) -m pytest -q

lint: ## Vérifie le style et les erreurs du code Python
	$(PY) -m ruff check .

clean: ## Supprime les données générées (sources/ et lake/)
	rm -rf sources lake

load-db: ## Copie les commandes de shop.db dans PostgreSQL
	$(PY) ingestion/charger_postgres.py

extract-orders: ## Extrait les nouvelles commandes de PostgreSQL vers lake/raw/
	$(PY) ingestion/extraire_commandes.py


extract-clients: ## Extrait le CSV des clients vers lake/raw/
	$(PY) ingestion/extraire_clients.py

extract-payments: ## Extrait les nouveaux fichiers de paiements vers lake/raw/
	$(PY) ingestion/extraire_paiements.py

extract-logs: ## Extrait les nouveaux fichiers de logs web vers lake/raw/
	$(PY) ingestion/extraire_logs.py

ingest: ## Collecte TOUTES les sources vers lake/raw/
	$(PY) ingestion/tout_extraire.py

extract-products: ## Récupère le catalogue produits depuis l API
	$(PY) ingestion/extraire_produits.py

spark-test: ## Vérifie que Spark et Delta Lake fonctionnent
	$(PY) spark_jobs/test_spark.py

explore: ## Explore le data lake avec Spark (lecture seule)
	$(PY) spark_jobs/explorer_lac.py

silver-orders: ## Construit la zone silver des commandes (Delta)
	$(PY) spark_jobs/silver_commandes.py

silver-products: ## Construit la zone silver des produits (Delta)
	$(PY) spark_jobs/silver_produits.py

silver-clients: ## Construit la zone silver des clients (Delta)
	$(PY) spark_jobs/silver_clients.py

silver-payments: ## Construit la zone silver des paiements (après silver-orders)
	$(PY) spark_jobs/silver_paiements.py

silver-logs: ## Construit la zone silver des logs web (Delta)
	$(PY) spark_jobs/silver_logs.py

delta-demo: ## Démonstration Delta Lake : MERGE et voyage dans le temps
	$(PY) spark_jobs/delta_demo.py

silver: silver-orders silver-order-items silver-products silver-clients silver-payments silver-logs ## Construit TOUTE la zone silver

dbt-debug: ## Vérifie que dbt trouve son projet et sa base DuckDB
	mkdir -p lake/gold
	cd dbt_shopflow && ../$(BIN)/dbt debug --profiles-dir .

dbt-run: ## Construit les modèles dbt dans lake/gold/shopflow.duckdb
	mkdir -p lake/gold
	cd dbt_shopflow && ../$(BIN)/dbt run --profiles-dir .

extract-order-items: ## Extrait toutes les lignes de commande de PostgreSQL vers lake/raw/
	$(PY) ingestion/extraire_lignes_commande.py

silver-order-items: ## Construit la zone silver des lignes de commande (après silver-orders)
	$(PY) spark_jobs/silver_lignes_commande.py

dbt-test: ## Lance les tests dbt
	cd dbt_shopflow && ../$(BIN)/dbt test --profiles-dir .

dbt-build: ## Construit ET teste tous les modèles dbt, dans le bon ordre
	mkdir -p lake/gold
	cd dbt_shopflow && ../$(BIN)/dbt build --profiles-dir .

dbt-docs: ## Génère et affiche la documentation dbt sur http://localhost:8080
	cd dbt_shopflow && ../$(BIN)/dbt docs generate --profiles-dir . && ../$(BIN)/dbt docs serve --profiles-dir . --port 8080 --no-browser
