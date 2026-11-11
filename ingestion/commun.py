"""Étape 1.9 — Les outils communs à tous les scripts d'ingestion.

Tout ce qui était recopié dans chaque script (connexion, état, colonnes
techniques, écriture Parquet) est rangé ici, UNE seule fois.
Chaque script d'extraction n'a plus qu'à s'occuper de ce qui lui est propre :
la lecture de sa source.
"""

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# --- Les chemins du projet ---------------------------------------------------
RACINE = Path(__file__).resolve().parents[1]
DOSSIER_SOURCES = RACINE / "sources"
DOSSIER_RAW = RACINE / "lake" / "raw"
DOSSIER_REJETS = RACINE / "lake" / "rejets"
FICHIER_ETAT = RACINE / "lake" / "_etat" / "watermarks.json"


# --- Les messages (logs) -----------------------------------------------------
def get_logger(nom: str) -> logging.Logger:
    """Renvoie un logger qui affiche des messages horodatés."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    return logging.getLogger(nom)


# --- La connexion à PostgreSQL -----------------------------------------------
def connexion_postgres() -> psycopg.Connection:
    """Ouvre une connexion à PostgreSQL avec les identifiants du fichier .env."""
    load_dotenv(RACINE / ".env")
    return psycopg.connect(
        host="localhost",
        port=5432,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


# --- La mémoire du pipeline (lake/_etat/watermarks.json) ---------------------
def lire_etat(cle: str, defaut=None):
    """Renvoie la valeur enregistrée pour cette clé, ou `defaut` s'il n'y en a pas."""
    if not FICHIER_ETAT.exists():
        return defaut
    return json.loads(FICHIER_ETAT.read_text()).get(cle, defaut)


def ecrire_etat(cle: str, valeur) -> None:
    """Enregistre la valeur de cette clé, sans toucher aux clés des autres sources."""
    FICHIER_ETAT.parent.mkdir(parents=True, exist_ok=True)
    etat = json.loads(FICHIER_ETAT.read_text()) if FICHIER_ETAT.exists() else {}
    etat[cle] = valeur
    ecrire_texte_atomique(FICHIER_ETAT, json.dumps(etat, indent=2, ensure_ascii=False))


# --- Les lots et les colonnes techniques -------------------------------------
def nouveau_lot() -> tuple[datetime, str]:
    """Renvoie l'heure actuelle (UTC) et un identifiant unique pour ce lot."""
    maintenant = datetime.now(UTC)
    return maintenant, f"{maintenant:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"


def ajouter_colonnes_techniques(lignes: list[dict], source: str, maintenant: datetime, lot: str) -> None:
    """Ajoute _ingested_at, _source et _batch_id à chaque ligne (traçabilité)."""
    for ligne in lignes:
        ligne["_ingested_at"] = maintenant
        ligne["_source"] = source
        ligne["_batch_id"] = lot


# --- L'écriture dans le data lake --------------------------------------------
def chemin_dans_le_lac(dossier: Path, maintenant: datetime, nom_fichier: str) -> Path:
    """Renvoie <dossier>/date=AAAA-MM-JJ/<nom_fichier>, en créant le dossier si besoin."""
    partition = dossier / f"date={maintenant:%Y-%m-%d}"
    partition.mkdir(parents=True, exist_ok=True)
    return partition / nom_fichier


def ecrire_parquet(lignes: list[dict], fichier: Path) -> None:
    """Écrit les lignes en Parquet, de façon atomique (fichier .tmp puis renommage)."""
    temporaire = fichier.with_suffix(".tmp")
    pq.write_table(pa.Table.from_pylist(lignes), temporaire)
    os.replace(temporaire, fichier)


def ecrire_texte_atomique(fichier: Path, texte: str) -> None:
    """Écrit un fichier texte de façon atomique."""
    temporaire = fichier.with_suffix(".tmp")
    temporaire.write_text(texte, encoding="utf-8")
    os.replace(temporaire, fichier)
