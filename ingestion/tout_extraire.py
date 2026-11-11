"""Étape 1.9 — Lance l'extraction de TOUTES les sources, l'une après l'autre.

Si une source échoue (par exemple l'API éteinte), les autres passent quand même,
puis un récapitulatif est affiché et le script se termine avec un code d'erreur.

Lancement :  make ingest
"""

import sys
import time

import extraire_clients
import extraire_commandes
import extraire_logs
import extraire_paiements
import extraire_produits
from commun import get_logger

log = get_logger("ingestion")

# La liste des sources à collecter : (nom affiché, fonction à lancer)
SOURCES = [
    ("Commandes  (PostgreSQL)", extraire_commandes.main),
    ("Produits   (API)", extraire_produits.main),
    ("Clients    (CSV)", extraire_clients.main),
    ("Paiements  (JSON SaaS)", extraire_paiements.main),
    ("Logs web   (texte)", extraire_logs.main),
]


def main() -> None:
    resultats = []
    for nom, extraire in SOURCES:
        log.info("──── %s ────", nom)
        debut = time.perf_counter()
        try:
            extraire()
            statut = "✅ OK"
        except Exception as erreur:  # une source en panne ne doit pas bloquer les autres
            log.error("Échec de %s : %s", nom, erreur)
            statut = f"❌ ÉCHEC ({type(erreur).__name__})"
        resultats.append((nom, statut, time.perf_counter() - debut))

    log.info("════ Récapitulatif ════")
    for nom, statut, duree in resultats:
        log.info("%-25s %-28s %5.1f s", nom, statut, duree)

    nb_echecs = sum("ÉCHEC" in statut for _, statut, _ in resultats)
    if nb_echecs:
        log.error("%s source(s) en échec sur %s", nb_echecs, len(resultats))
        sys.exit(1)
    log.info("Toutes les sources ont été collectées.")


if __name__ == "__main__":
    main()
