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
