"""Commandes — extraction INCRÉMENTALE depuis PostgreSQL (watermark sur updated_at).

Écrit dans : lake/raw/orders/date=AAAA-MM-JJ/orders_<lot>.parquet
Lancement  : make extract-orders
"""

from datetime import datetime

from psycopg.rows import dict_row

from commun import (
    DOSSIER_RAW,
    RACINE,
    ajouter_colonnes_techniques,
    chemin_dans_le_lac,
    connexion_postgres,
    ecrire_etat,
    ecrire_parquet,
    get_logger,
    lire_etat,
    nouveau_lot,
)

log = get_logger("commandes")
CLE_ETAT = "orders"
WATERMARK_INITIAL = "1970-01-01T00:00:00"

REQUETE_EXTRACTION = """
SELECT order_id, customer_id, order_date, status, total_amount,
       currency, warehouse, updated_at
FROM orders
WHERE updated_at > %(watermark)s     -- seulement ce qui a changé depuis la dernière fois
  AND updated_at <= LOCALTIMESTAMP   -- on écarte les dates dans le futur (données corrompues)
ORDER BY updated_at
"""
REQUETE_DATES_FUTURES = "SELECT count(*) AS nb FROM orders WHERE updated_at > LOCALTIMESTAMP"


def main() -> None:
    watermark = datetime.fromisoformat(lire_etat(CLE_ETAT, WATERMARK_INITIAL))
    log.info("Watermark actuel : %s", watermark)

    # 1. EXTRAIRE ce qui a changé depuis le watermark (la partie propre à cette source)
    with connexion_postgres() as con, con.cursor(row_factory=dict_row) as cur:
        cur.execute(REQUETE_DATES_FUTURES)
        nb_futur = cur.fetchone()["nb"]
        if nb_futur:
            log.warning("%s commandes ont une date de modification dans le futur : ignorées", nb_futur)
        cur.execute(REQUETE_EXTRACTION, {"watermark": watermark})
        lignes = cur.fetchall()

    if not lignes:
        log.info("Aucune nouvelle commande : rien à faire.")
        return

    # 2. Colonnes techniques, 3. écriture, 4. SEULEMENT APRÈS, le watermark avance
    maintenant, lot = nouveau_lot()
    ajouter_colonnes_techniques(lignes, "postgres.orders", maintenant, lot)
    fichier = chemin_dans_le_lac(DOSSIER_RAW / "orders", maintenant, f"orders_{lot}.parquet")
    ecrire_parquet(lignes, fichier)
    log.info("%s commandes écrites dans %s", len(lignes), fichier.relative_to(RACINE))

    nouveau_watermark = lignes[-1]["updated_at"]
    ecrire_etat(CLE_ETAT, nouveau_watermark.isoformat())
    log.info("Nouveau watermark : %s", nouveau_watermark)


if __name__ == "__main__":
    main()
