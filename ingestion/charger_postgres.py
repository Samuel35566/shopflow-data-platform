"""Étape 1.3 — Copie les commandes de sources/shop.db dans PostgreSQL.

On simule ainsi la base de production de ShopFlow : un vrai PostgreSQL,
comme dans une entreprise. Le script peut être relancé sans risque :
les tables sont supprimées puis recréées à chaque exécution.

Lancement :  make load-db
"""

import logging
import os
import sqlite3
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

# Dossier racine du projet (le dossier parent de ingestion/)
RACINE = Path(__file__).resolve().parents[1]
FICHIER_SQLITE = RACINE / "sources" / "shop.db"

# Des messages horodatés plutôt que des print() : c'est la norme en production
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("charger_postgres")

# Structure des tables dans PostgreSQL, avec des types précis
# (dans shop.db, les dates sont stockées en texte : ici, ce sont de vraies dates)
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

-- Index sur updated_at : il rendra rapide l'extraction incrémentale (étape 1.4)
CREATE INDEX idx_orders_updated_at ON orders (updated_at);
"""

# Les colonnes à copier pour chaque table
TABLES = {
    "orders": ["order_id", "customer_id", "order_date", "status",
               "total_amount", "currency", "warehouse", "updated_at"],
    "order_items": ["order_id", "line_number", "product_id",
                    "quantity", "unit_price", "discount"],
}


def connexion_postgres() -> psycopg.Connection:
    """Ouvre une connexion à PostgreSQL avec les identifiants du fichier .env."""
    load_dotenv(RACINE / ".env")
    return psycopg.connect(
        host="localhost",
        port=5432,
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
    )


def copier_table(sqlite_con: sqlite3.Connection, pg_cur: psycopg.Cursor, table: str) -> int:
    """Copie une table de SQLite vers PostgreSQL et renvoie le nombre de lignes copiées."""
    colonnes = ", ".join(TABLES[table])
    lignes = sqlite_con.execute(f"SELECT {colonnes} FROM {table}")

    # COPY est la façon la plus rapide de charger beaucoup de lignes dans PostgreSQL :
    # bien plus rapide que 50 000 INSERT successifs.
    nb = 0
    with pg_cur.copy(f"COPY {table} ({colonnes}) FROM STDIN") as copie:
        for ligne in lignes:
            copie.write_row(ligne)
            nb += 1
    return nb


def main() -> None:
    if not FICHIER_SQLITE.exists():
        log.error("Fichier %s introuvable : lance d'abord `make data`.", FICHIER_SQLITE)
        sys.exit(1)

    sqlite_con = sqlite3.connect(FICHIER_SQLITE)

    # Tout se passe dans UNE transaction : si une erreur survient au milieu,
    # PostgreSQL annule tout et on ne se retrouve jamais avec une base à moitié chargée.
    with connexion_postgres() as pg_con, pg_con.cursor() as pg_cur:
        log.info("Création des tables…")
        pg_cur.execute(CREATION_TABLES)

        for table in TABLES:
            nb = copier_table(sqlite_con, pg_cur, table)
            log.info("Table %-12s : %s lignes copiées", table, f"{nb:,}".replace(",", " "))

        # Vérification : on recompte directement dans PostgreSQL
        pg_cur.execute("SELECT count(*), count(DISTINCT order_id) FROM orders")
        total, distinctes = pg_cur.fetchone()
        log.info("Contrôle : %s lignes dans orders, dont %s commandes distinctes", total, distinctes)
    # En sortant du bloc `with` sans erreur, la transaction est validée (commit)

    sqlite_con.close()
    log.info("Terminé : la base PostgreSQL de ShopFlow est prête.")


if __name__ == "__main__":
    main()