"""Étape 3.2 — Lignes de commande : extraction COMPLÈTE (snapshot) depuis PostgreSQL.

La table order_items n'a PAS de colonne updated_at : impossible de savoir ce qui a changé.
On recopie donc toute la table à chaque fois (comme le catalogue produits),
et la zone silver ne gardera que le snapshot le plus récent.

Écrit dans : lake/raw/order_items/date=AAAA-MM-JJ/order_items.parquet (remplacé si relancé le même jour)
Lancement  : make extract-order-items
"""

from psycopg.rows import dict_row

from commun import (
    DOSSIER_RAW,
    RACINE,
    ajouter_colonnes_techniques,
    chemin_dans_le_lac,
    connexion_postgres,
    ecrire_parquet,
    get_logger,
    nouveau_lot,
)

log = get_logger("lignes_commande")

REQUETE = """
SELECT order_id, line_number, product_id, quantity, unit_price, discount
FROM order_items
ORDER BY order_id, line_number
"""


def main() -> None:
    # 1. EXTRAIRE toute la table
    with connexion_postgres() as con, con.cursor(row_factory=dict_row) as cur:
        cur.execute(REQUETE)
        lignes = cur.fetchall()

    # Un snapshot vide n'est pas « rien de nouveau » : c'est anormal, on s'arrête
    if not lignes:
        raise ValueError("La table order_items est vide : lance d'abord `make load-db`.")

    # 2. Colonnes techniques, 3. écriture
    maintenant, lot = nouveau_lot()
    ajouter_colonnes_techniques(lignes, "postgres.order_items", maintenant, lot)
    fichier = chemin_dans_le_lac(DOSSIER_RAW / "order_items", maintenant, "order_items.parquet")
    ecrire_parquet(lignes, fichier)
    log.info("%s lignes de commande écrites dans %s", len(lignes), fichier.relative_to(RACINE))


if __name__ == "__main__":
    main()