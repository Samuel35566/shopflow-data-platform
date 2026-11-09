"""Premier test automatique : le générateur crée bien toutes les sources."""

import sqlite3
import subprocess
import sys
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent


def test_generateur_cree_les_sources(tmp_path):
    # On lance le générateur dans un dossier temporaire, avec peu de lignes pour aller vite
    subprocess.run(
        [sys.executable, str(RACINE / "generer_donnees.py"), "--lignes", "500"],
        cwd=tmp_path, check=True, capture_output=True,
    )
    sources = tmp_path / "sources"

    assert (sources / "shop.db").exists()
    assert (sources / "clients.csv").exists()
    assert any((sources / "paiements").glob("*.json"))
    assert any((sources / "logs").glob("*.log"))

    # 500 commandes distinctes, plus quelques doublons volontaires
    with sqlite3.connect(sources / "shop.db") as con:
        total, distinctes = con.execute(
            "SELECT count(*), count(DISTINCT order_id) FROM orders"
        ).fetchone()
    assert distinctes == 500
    assert total >= distinctes
