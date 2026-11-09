"""Étape 1.7 — Extraction des paiements exportés par l'outil SaaS (façon Stripe).

Chaque jour, l'outil de paiement dépose UN fichier JSON dans sources/paiements/ :
    paiements_AAAA-MM-JJ.json

On traite les fichiers de façon INCRÉMENTALE : on garde la liste des fichiers
déjà traités, et à chaque exécution on ne lit que les nouveaux.
Les paiements sont déposés tels quels dans le data lake :
    lake/raw/payments/date=AAAA-MM-JJ/payments_<lot>.parquet

Lancement :  make extract-payments
"""

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

RACINE = Path(__file__).resolve().parents[1]
DOSSIER_SOURCE = RACINE / "sources" / "paiements"
DOSSIER_RAW = RACINE / "lake" / "raw" / "payments"
FICHIER_ETAT = RACINE / "lake" / "_etat" / "watermarks.json"
CLE_ETAT = "paiements_fichiers_traites"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("extraire_paiements")


def lire_fichiers_traites() -> set[str]:
    """Renvoie l'ensemble des noms de fichiers déjà traités lors des exécutions précédentes."""
    if not FICHIER_ETAT.exists():
        return set()
    etat = json.loads(FICHIER_ETAT.read_text())
    return set(etat.get(CLE_ETAT, []))


def enregistrer_fichiers_traites(fichiers: set[str]) -> None:
    """Enregistre la liste des fichiers traités, sans effacer l'état des autres sources."""
    FICHIER_ETAT.parent.mkdir(parents=True, exist_ok=True)
    etat = json.loads(FICHIER_ETAT.read_text()) if FICHIER_ETAT.exists() else {}
    etat[CLE_ETAT] = sorted(fichiers)
    temporaire = FICHIER_ETAT.with_suffix(".tmp")
    temporaire.write_text(json.dumps(etat, indent=2))
    os.replace(temporaire, FICHIER_ETAT)


def lire_paiements(fichier: Path) -> list[dict]:
    """Lit un fichier JSON de paiements et renvoie une ligne « à plat » par paiement."""
    contenu = json.loads(fichier.read_text(encoding="utf-8"))
    paiements = []
    for p in contenu["data"]:
        paiements.append({
            "payment_id": p["id"],
            "amount": p["amount"],          # en CENTIMES (4599 = 45,99), comme chez Stripe
            "currency": p["currency"],
            "created": p["created"],        # date au format « timestamp Unix » (secondes depuis 1970)
            "status": p["status"],
            # Le numéro de commande est rangé dans un sous-objet "metadata" : on le remonte
            # au premier niveau pour avoir une simple colonne (on « aplatit » le JSON).
            "order_id": p.get("metadata", {}).get("order_id"),
            "_fichier_source": fichier.name,
        })
    return paiements


def main() -> None:
    deja_traites = lire_fichiers_traites()
    tous_les_fichiers = sorted(DOSSIER_SOURCE.glob("paiements_*.json"))
    nouveaux = [f for f in tous_les_fichiers if f.name not in deja_traites]

    log.info("%s fichiers dans la source, %s déjà traités, %s nouveaux",
             len(tous_les_fichiers), len(deja_traites), len(nouveaux))
    if not nouveaux:
        log.info("Aucun nouveau fichier de paiements : rien à faire.")
        return

    # 1. LIRE tous les nouveaux fichiers
    paiements = []
    for fichier in nouveaux:
        paiements.extend(lire_paiements(fichier))

    # 2. Colonnes techniques
    maintenant = datetime.now(UTC)
    lot = f"{maintenant:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    for p in paiements:
        p["_ingested_at"] = maintenant
        p["_source"] = "saas.paiements"
        p["_batch_id"] = lot

    # 3. ÉCRIRE dans le data lake (un fichier par lot)
    dossier = DOSSIER_RAW / f"date={maintenant:%Y-%m-%d}"
    dossier.mkdir(parents=True, exist_ok=True)
    fichier = dossier / f"payments_{lot}.parquet"
    temporaire = fichier.with_suffix(".tmp")
    pq.write_table(pa.Table.from_pylist(paiements), temporaire)
    os.replace(temporaire, fichier)
    log.info("%s paiements (%s fichiers) écrits dans %s",
             len(paiements), len(nouveaux), fichier.relative_to(RACINE))

    # 4. SEULEMENT APRÈS : noter ces fichiers comme traités
    enregistrer_fichiers_traites(deja_traites | {f.name for f in nouveaux})


if __name__ == "__main__":
    main()
