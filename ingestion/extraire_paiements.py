"""Paiements — extraction INCRÉMENTALE des fichiers JSON déposés par l'outil SaaS.

On garde la liste des fichiers déjà traités et on ne lit que les nouveaux.
Écrit dans : lake/raw/payments/date=AAAA-MM-JJ/payments_<lot>.parquet
Lancement  : make extract-payments
"""

import json
from pathlib import Path

from commun import (
    DOSSIER_RAW,
    DOSSIER_SOURCES,
    RACINE,
    ajouter_colonnes_techniques,
    chemin_dans_le_lac,
    ecrire_etat,
    ecrire_parquet,
    get_logger,
    lire_etat,
    nouveau_lot,
)

log = get_logger("paiements")
DOSSIER_SOURCE = DOSSIER_SOURCES / "paiements"
CLE_ETAT = "paiements_fichiers_traites"


def lire_paiements(fichier: Path) -> list[dict]:
    """Lit un fichier JSON de paiements et renvoie une ligne « à plat » par paiement."""
    contenu = json.loads(fichier.read_text(encoding="utf-8"))
    return [
        {
            "payment_id": p["id"],
            "amount": p["amount"],          # en CENTIMES (4599 = 45,99)
            "currency": p["currency"],
            "created": p["created"],        # timestamp Unix (secondes depuis 1970)
            "status": p["status"],
            "order_id": p.get("metadata", {}).get("order_id"),  # on « aplatit » le JSON
            "_fichier_source": fichier.name,
        }
        for p in contenu["data"]
    ]


def main() -> None:
    deja_traites = set(lire_etat(CLE_ETAT, []))
    tous_les_fichiers = sorted(DOSSIER_SOURCE.glob("paiements_*.json"))
    nouveaux = [f for f in tous_les_fichiers if f.name not in deja_traites]
    log.info("%s fichiers dans la source, %s déjà traités, %s nouveaux",
             len(tous_les_fichiers), len(deja_traites), len(nouveaux))
    if not nouveaux:
        log.info("Aucun nouveau fichier de paiements : rien à faire.")
        return

    paiements = []
    for fichier in nouveaux:
        paiements.extend(lire_paiements(fichier))

    maintenant, lot = nouveau_lot()
    ajouter_colonnes_techniques(paiements, "saas.paiements", maintenant, lot)
    fichier = chemin_dans_le_lac(DOSSIER_RAW / "payments", maintenant, f"payments_{lot}.parquet")
    ecrire_parquet(paiements, fichier)
    log.info("%s paiements (%s fichiers) écrits dans %s", len(paiements), len(nouveaux), fichier.relative_to(RACINE))

    # SEULEMENT APRÈS l'écriture : noter ces fichiers comme traités
    ecrire_etat(CLE_ETAT, sorted(deja_traites | {f.name for f in nouveaux}))


if __name__ == "__main__":
    main()
