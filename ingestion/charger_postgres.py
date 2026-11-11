"""Mise en place — copie les commandes de sources/shop.db dans PostgreSQL.

Ce n'est PAS une extraction : ce script fabrique la « base de production »
de ShopFlow, que les scripts d'extraction viennent ensuite lire.
Relançable sans risque : les tables sont supprimées puis recréées.

Lancement :  make load-db
"""

import sqlite3

import psycopg

from commun import DOSSIER_SOURCES, connexion_postgres, get_logger

log = get_logger("charger_postgres")
FICHIER_SQLITE = DOSSIER_SOURCES / "shop.db"

CREATION_TABLES = """
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id      INTEGER        NOT NULL,
    customer_id   INTEGER,
    order_date    TIMESTAMP,
    status        TEXT,
    total_amount  NUMERIC(12, 2),
    currency      TEXT,
    warehouse     TEXT,
    updated_at    TIMESTAMP      NOT NULL
);

CREATE TABLE order_items (
    order_id      INTEGER        NOT NULL,
    line_number   INTEGER        NOT NULL,
    product_id    INTEGER,
    quantity      INTEGER,
    unit_price    NUMERIC(10, 2),
    discount      NUMERIC(4, 2)
);

-- Index sur updated_at : il rend rapide l'extraction incrémentale
CREATE INDEX idx_orders_updated_at ON orders (updated_at);
"""

TABLES = {
    "orders": ["order_id", "customer_id", "order_date", "status",
               "total_amount", "currency", "warehouse", "updated_at"],
    "order_items": ["order_id", "line_number", "product_id",
                    "quantity", "unit_price", "discount"],
}


def copier_table(sqlite_con: sqlite3.Connection, pg_cur: psycopg.Cursor, table: str) -> int:
    """Copie une table de SQLite vers PostgreSQL (avec COPY) et renvoie le nombre de lignes."""
    colonnes = ", ".join(TABLES[table])
    nb = 0
    with pg_cur.copy(f"COPY {table} ({colonnes}) FROM STDIN") as copie:
        for ligne in sqlite_con.execute(f"SELECT {colonnes} FROM {table}"):
            copie.write_row(ligne)
            nb += 1
    return nb


def main() -> None:
    if not FICHIER_SQLITE.exists():
        raise FileNotFoundError(f"{FICHIER_SQLITE} introuvable : lance d'abord `make data`.")

    sqlite_con = sqlite3.connect(FICHIER_SQLITE)
    # Une seule transaction : tout ou rien
    with connexion_postgres() as pg_con, pg_con.cursor() as pg_cur:
        log.info("Création des tables…")
        pg_cur.execute(CREATION_TABLES)
        for table in TABLES:
            log.info("Table %-12s : %s lignes copiées", table, copier_table(sqlite_con, pg_cur, table))
        pg_cur.execute("SELECT count(*), count(DISTINCT order_id) FROM orders")
        total, distinctes = pg_cur.fetchone()
        log.info("Contrôle : %s lignes dans orders, dont %s commandes distinctes", total, distinctes)
    sqlite_con.close()
    log.info("Terminé : la base PostgreSQL de ShopFlow est prête.")


if __name__ == "__main__":
    main()
