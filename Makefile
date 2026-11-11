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
