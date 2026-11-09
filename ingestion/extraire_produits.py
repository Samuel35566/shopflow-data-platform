"""Étape 1.5 — Extraction COMPLÈTE des produits depuis l'API interne.

L'API renvoie les produits par pages de 50. On les parcourt toutes, en
réessayant en cas d'erreur temporaire (429, 5xx), puis on dépose le
catalogue complet (un « snapshot ») dans le data lake :
    lake/raw/products/date=AAAA-MM-JJ/products.parquet

Prérequis : l'API doit tourner (python generer_donnees.py --api)
Lancement :  make extract-products
"""

import logging
import os
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

RACINE = Path(__file__).resolve().parents[1]
URL_API = "http://localhost:8000/products"
DOSSIER_RAW = RACINE / "lake" / "raw" / "products"

MAX_ESSAIS = 5                                  # nombre maximum de tentatives par page
CODES_A_REESSAYER = {429, 500, 502, 503, 504}   # erreurs temporaires

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("extraire_produits")


def get_page(session: requests.Session, page: int) -> dict:
    """Récupère UNE page de l'API, en réessayant en cas d'erreur temporaire.

    - code 200                        -> renvoyer le JSON de la réponse
    - code dans CODES_A_REESSAYER     -> attendre (1 s, 2 s, 4 s, 8 s…) puis réessayer
    - autre code (ex : 404)           -> erreur définitive : reponse.raise_for_status()
    - après MAX_ESSAIS échecs         -> lever une RuntimeError
    """
    for essai in range(1, MAX_ESSAIS + 1):
        reponse = session.get(URL_API, params={"page": page}, timeout=10)

        if reponse.status_code == 200:
            return reponse.json()

        if reponse.status_code not in CODES_A_REESSAYER:
            reponse.raise_for_status()

        attente = 2 ** (essai - 1)
        log.warning(
            "Page %s : erreur %s (essai %s/%s), nouvel essai dans %s s",
            page, reponse.status_code, essai, MAX_ESSAIS, attente,
        )
        time.sleep(attente)

    raise RuntimeError(f"Page {page} : échec après {MAX_ESSAIS} essais")


def recuperer_tous_les_produits(session: requests.Session) -> list[dict]:
    """Parcourt toutes les pages de l'API et renvoie la liste complète des produits."""
    produits = []
    page = 1

    while page is not None:
        reponse = get_page(session, page)
        produits.extend(reponse["data"])
        log.info("Page %s : %s produits", page, len(reponse["data"]))
        page = reponse["next_page"]

    return produits


def main() -> None:
    try:
        with requests.Session() as session:
            produits = recuperer_tous_les_produits(session)
    except requests.ConnectionError:
        log.error("API injoignable : lance d'abord `python generer_donnees.py --api` dans un autre terminal.")
        sys.exit(1)

    # Colonnes techniques, comme pour les commandes
    maintenant = datetime.now(UTC)
    lot = f"{maintenant:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    for produit in produits:
        produit["_ingested_at"] = maintenant
        produit["_source"] = "api.products"
        produit["_batch_id"] = lot

    # Un seul fichier par jour : relancer le même jour REMPLACE le fichier (idempotence)
    dossier = DOSSIER_RAW / f"date={maintenant:%Y-%m-%d}"
    dossier.mkdir(parents=True, exist_ok=True)
    fichier = dossier / "products.parquet"
    temporaire = fichier.with_suffix(".tmp")
    pq.write_table(pa.Table.from_pylist(produits), temporaire)
    os.replace(temporaire, fichier)
    log.info("%s produits écrits dans %s", len(produits), fichier.relative_to(RACINE))


if __name__ == "__main__":
    main()
