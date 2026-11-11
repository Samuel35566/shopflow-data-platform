"""Clients — extraction du CSV exporté par le CRM (encodage cp1252, séparateur « ; »).

Écrit dans : lake/raw/clients/date=AAAA-MM-JJ/clients.parquet (remplacé si relancé le même jour)
Lancement  : make extract-clients
"""

import csv
from pathlib import Path

from commun import (
    DOSSIER_RAW,
    DOSSIER_SOURCES,
    RACINE,
    ajouter_colonnes_techniques,
    chemin_dans_le_lac,
    ecrire_parquet,
    get_logger,
    nouveau_lot,
)

log = get_logger("clients")
FICHIER_CSV = DOSSIER_SOURCES / "clients.csv"
ENCODAGES_A_ESSAYER = ["utf-8", "cp1252"]

# Les colonnes attendues dans le fichier, et leur nom dans le data lake
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
    """Lit le CSV, vérifie ses colonnes et renvoie une liste de dictionnaires renommés."""
    with open(chemin, encoding=encodage, newline="") as f:
        lecteur = csv.DictReader(f, delimiter=";")
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
        raise FileNotFoundError(f"{FICHIER_CSV} introuvable : lance d'abord `make data`.")

    encodage = detecter_encodage(FICHIER_CSV)
    log.info("Encodage détecté : %s", encodage)
    clients = lire_clients(FICHIER_CSV, encodage)

    maintenant, lot = nouveau_lot()
    ajouter_colonnes_techniques(clients, "crm.clients_csv", maintenant, lot)
    fichier = chemin_dans_le_lac(DOSSIER_RAW / "clients", maintenant, "clients.parquet")
    ecrire_parquet(clients, fichier)
    log.info("%s clients écrits dans %s", len(clients), fichier.relative_to(RACINE))


if __name__ == "__main__":
    main()
