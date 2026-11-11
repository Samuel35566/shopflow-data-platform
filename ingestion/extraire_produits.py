"""Produits — extraction COMPLÈTE (snapshot) depuis l'API, avec pagination et réessais.

Prérequis  : l'API doit tourner (python generer_donnees.py --api)
Écrit dans : lake/raw/products/date=AAAA-MM-JJ/products.parquet (remplacé si relancé le même jour)
Lancement  : make extract-products
"""

import time

import requests

from commun import (
    DOSSIER_RAW,
    RACINE,
    ajouter_colonnes_techniques,
    chemin_dans_le_lac,
    ecrire_parquet,
    get_logger,
    nouveau_lot,
)

log = get_logger("produits")
URL_API = "http://localhost:8000/products"
MAX_ESSAIS = 5                                  # nombre maximum de tentatives par page
CODES_A_REESSAYER = {429, 500, 502, 503, 504}   # erreurs temporaires


def get_page(session: requests.Session, page: int) -> dict:
    """Récupère UNE page de l'API, en réessayant en cas d'erreur temporaire."""
    for essai in range(1, MAX_ESSAIS + 1):
        reponse = session.get(URL_API, params={"page": page}, timeout=10)
        if reponse.status_code == 200:
            return reponse.json()
        if reponse.status_code not in CODES_A_REESSAYER:
            reponse.raise_for_status()
        attente = 2 ** (essai - 1)
        log.warning("Page %s : erreur %s (essai %s/%s), nouvel essai dans %s s",
                    page, reponse.status_code, essai, MAX_ESSAIS, attente)
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
    except requests.ConnectionError as erreur:
        raise RuntimeError("API injoignable : lance d'abord `python generer_donnees.py --api`") from erreur

    # En raw, le prix reste du texte : il est tantôt un nombre, tantôt "12,50 €"
    for produit in produits:
        if produit["price"] is not None:
            produit["price"] = str(produit["price"])

    maintenant, lot = nouveau_lot()
    ajouter_colonnes_techniques(produits, "api.products", maintenant, lot)
    fichier = chemin_dans_le_lac(DOSSIER_RAW / "products", maintenant, "products.parquet")
    ecrire_parquet(produits, fichier)
    log.info("%s produits écrits dans %s", len(produits), fichier.relative_to(RACINE))


if __name__ == "__main__":
    main()
