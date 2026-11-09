"""Étape 1.4 — Extraction INCRÉMENTALE des commandes depuis PostgreSQL.

À chaque exécution, on ne récupère QUE les commandes créées ou modifiées
depuis le passage précédent. Pour s'en souvenir, on stocke un « watermark » :
la date de modification la plus récente déjà extraite.

Les données sont déposées telles quelles (brutes) dans le data lake :
    lake/raw/orders/date=AAAA-MM-JJ/orders_<lot>.parquet

Lancement :  make extract-orders
"""

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from psycopg.rows import dict_row

RACINE = Path(__file__).resolve().parents[1]
DOSSIER_RAW = RACINE / "lake" / "raw" / "orders"
FICHIER_ETAT = RACINE / "lake" / "_etat" / "watermarks.json"

# Watermark de départ : une date très ancienne, pour tout récupérer la première fois
WATERMARK_INITIAL = datetime(1970, 1, 1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("extraire_commandes")

# %(watermark)s est un PARAMÈTRE : psycopg y place la valeur de façon sûre.
# (Ne jamais coller une valeur dans du SQL avec un f-string : risque d'injection SQL.)
REQUETE_EXTRACTION = """
SELECT order_id, customer_id, order_date, status, total_amount,
       currency, warehouse, updated_at
FROM orders
WHERE updated_at > %(watermark)s     -- seulement ce qui a changé depuis la dernière fois
  AND updated_at <= LOCALTIMESTAMP   -- on écarte les dates dans le futur (données corrompues)
ORDER BY updated_at
"""

REQUETE_DATES_FUTURES = "SELECT count(*) AS nb FROM orders WHERE updated_at > LOCALTIMESTAMP"


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


def lire_watermark() -> datetime:
    """Renvoie le watermark enregistré, ou le watermark initial s'il n'y en a pas encore."""
    if not FICHIER_ETAT.exists():
        return WATERMARK_INITIAL
    etat = json.loads(FICHIER_ETAT.read_text())
    return datetime.fromisoformat(etat.get("orders", WATERMARK_INITIAL.isoformat()))


def ecrire_watermark(nouveau: datetime) -> None:
    """Enregistre le nouveau watermark, sans effacer ceux des autres sources."""
    FICHIER_ETAT.parent.mkdir(parents=True, exist_ok=True)
    etat = json.loads(FICHIER_ETAT.read_text()) if FICHIER_ETAT.exists() else {}
    etat["orders"] = nouveau.isoformat()

    # Écriture « atomique » : on écrit dans un fichier temporaire puis on le renomme.
    # Si le script plante pendant l'écriture, l'ancien fichier reste intact.
    temporaire = FICHIER_ETAT.with_suffix(".tmp")
    temporaire.write_text(json.dumps(etat, indent=2))
    os.replace(temporaire, FICHIER_ETAT)


def ecrire_parquet(lignes: list[dict], lot: str, maintenant: datetime) -> Path:
    """Écrit les lignes dans un fichier Parquet du data lake et renvoie son chemin."""
    dossier = DOSSIER_RAW / f"date={maintenant:%Y-%m-%d}"
    dossier.mkdir(parents=True, exist_ok=True)
    fichier = dossier / f"orders_{lot}.parquet"

    table = pa.Table.from_pylist(lignes)
    temporaire = fichier.with_suffix(".tmp")
    pq.write_table(table, temporaire)
    os.replace(temporaire, fichier)
    return fichier


def main() -> None:
    watermark = lire_watermark()
    log.info("Watermark actuel : %s", watermark)

    # 1. EXTRAIRE : on lit dans PostgreSQL ce qui a changé depuis le watermark
    with connexion_postgres() as con, con.cursor(row_factory=dict_row) as cur:
        cur.execute(REQUETE_DATES_FUTURES)
        nb_futur = cur.fetchone()["nb"]
        if nb_futur:
            log.warning("%s commandes ont une date de modification dans le futur : ignorées", nb_futur)

        cur.execute(REQUETE_EXTRACTION, {"watermark": watermark})
        lignes = cur.fetchall()

    if not lignes:
        log.info("Aucune nouvelle commande depuis %s : rien à faire.", watermark)
        return

    # 2. AJOUTER les colonnes techniques : quand, d'où, et dans quel lot
    maintenant = datetime.now(UTC)
    lot = f"{maintenant:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    for ligne in lignes:
        ligne["_ingested_at"] = maintenant
        ligne["_source"] = "postgres.orders"
        ligne["_batch_id"] = lot

    # 3. ÉCRIRE dans le data lake…
    fichier = ecrire_parquet(lignes, lot, maintenant)
    log.info("%s commandes écrites dans %s", len(lignes), fichier.relative_to(RACINE))

    # 4. … et SEULEMENT APRÈS, avancer le watermark.
    # Si on l'avançait avant et que l'écriture plantait, ces commandes seraient perdues.
    nouveau_watermark = lignes[-1]["updated_at"]  # la plus récente (requête triée)
    ecrire_watermark(nouveau_watermark)
    log.info("Nouveau watermark : %s", nouveau_watermark)


if __name__ == "__main__":
    main()