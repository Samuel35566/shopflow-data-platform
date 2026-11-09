"""Étape 1.6 — Extraction du fichier CSV des clients (export du CRM).

Le fichier sources/clients.csv a deux particularités, comme beaucoup
d'exports réels :
  - il est encodé en cp1252 (ancien encodage Windows), pas en UTF-8 ;
  - ses colonnes sont séparées par des points-virgules (;), pas des virgules.

On le dépose tel quel (toutes les valeurs restent du texte) dans le data lake :
    lake/raw/clients/date=AAAA-MM-JJ/clients.parquet

Lancement :  make extract-clients
"""

import csv
import logging
import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

RACINE = Path(__file__).resolve().parents[1]
FICHIER_CSV = RACINE / "sources" / "clients.csv"
DOSSIER_RAW = RACINE / "lake" / "raw" / "clients"

# Les encodages à essayer, dans l'ordre : d'abord le standard moderne (UTF-8),
# puis l'ancien encodage Windows, très fréquent dans les exports d'outils métier.
ENCODAGES_A_ESSAYER = ["utf-8", "cp1252"]

# Les colonnes attendues dans le fichier, et le nom qu'on leur donne dans le data lake
# (sans espace ni accent : plus simple à manipuler en SQL et en Python).
COLONNES = {
    "ID Client": "client_id",
    "Prénom": "prenom",
    "Nom": "nom",
    "E-mail": "email",
    "Téléphone": "telephone",
    "Pays": "pays",
    "Date inscription": "date_inscription",
    "Segment": "segment",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("extraire_clients")


def detecter_encodage(chemin: Path) -> str:
    """Renvoie le premier encodage de la liste qui permet de lire le fichier sans erreur."""
    contenu = chemin.read_bytes()
    for encodage in ENCODAGES_A_ESSAYER:
        try:
            contenu.decode(encodage)  # « strict » : échoue au moindre octet invalide
            return encodage
        except UnicodeDecodeError:
            log.info("Le fichier n'est pas en %s, essai suivant…", encodage)
    raise ValueError(f"Encodage inconnu pour {chemin} (essayés : {ENCODAGES_A_ESSAYER})")


def lire_clients(chemin: Path, encodage: str) -> list[dict]:
    """Lit le CSV et renvoie une liste de dictionnaires, avec les colonnes renommées."""
    with open(chemin, encoding=encodage, newline="") as f:
        lecteur = csv.DictReader(f, delimiter=";")

        # Contrôle du schéma : si l'outil source change ses colonnes, on s'arrête
        # tout de suite avec un message clair, au lieu de produire des données fausses.
        colonnes_recues = set(lecteur.fieldnames or [])
        if colonnes_recues != set(COLONNES):
            raise ValueError(
                f"Colonnes inattendues dans {chemin.name}.\n"
                f"  Manquantes : {sorted(set(COLONNES) - colonnes_recues)}\n"
                f"  En trop    : {sorted(colonnes_recues - set(COLONNES))}"
            )

        return [{COLONNES[nom]: valeur for nom, valeur in ligne.items()} for ligne in lecteur]


def main() -> None:
    if not FICHIER_CSV.exists():
        log.error("Fichier %s introuvable : lance d'abord `make data`.", FICHIER_CSV)
        sys.exit(1)

    encodage = detecter_encodage(FICHIER_CSV)
    log.info("Encodage détecté : %s", encodage)

    clients = lire_clients(FICHIER_CSV, encodage)
    log.info("%s clients lus", len(clients))

    # Colonnes techniques
    maintenant = datetime.now(UTC)
    lot = f"{maintenant:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    for client in clients:
        client["_ingested_at"] = maintenant
        client["_source"] = "crm.clients_csv"
        client["_batch_id"] = lot

    # Un seul fichier par jour : relancer le même jour le remplace (idempotence)
    dossier = DOSSIER_RAW / f"date={maintenant:%Y-%m-%d}"
    dossier.mkdir(parents=True, exist_ok=True)
    fichier = dossier / "clients.parquet"
    temporaire = fichier.with_suffix(".tmp")
    pq.write_table(pa.Table.from_pylist(clients), temporaire)
    os.replace(temporaire, fichier)
    log.info("%s clients écrits dans %s", len(clients), fichier.relative_to(RACINE))


if __name__ == "__main__":
    main()