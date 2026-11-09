"""Étape 1.8 — Extraction des logs du serveur web (navigation des visiteurs).

Chaque jour, le serveur web écrit un fichier texte sources/logs/access_AAAA-MM-JJ.log,
avec une ligne par page visitée, par exemple :

  154.210.254.76 - - [01/Jan/2026:11:34:09 +0100] "GET /produits HTTP/1.1" 200 12158 "-" "Mozilla/5.0 ..."

On découpe chaque ligne en colonnes grâce à une EXPRESSION RÉGULIÈRE.
  - Les lignes valides vont dans le data lake :  lake/raw/web_logs/date=AAAA-MM-JJ/web_logs_<lot>.parquet
  - Les lignes illisibles sont mises de côté :   lake/rejets/web_logs/date=AAAA-MM-JJ/rejets_<lot>.jsonl
Les fichiers sont traités de façon incrémentale (liste des fichiers déjà traités).

Lancement :  make extract-logs
"""

import json
import logging
import os
import re
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

RACINE = Path(__file__).resolve().parents[1]
DOSSIER_SOURCE = RACINE / "sources" / "logs"
DOSSIER_RAW = RACINE / "lake" / "raw" / "web_logs"
DOSSIER_REJETS = RACINE / "lake" / "rejets" / "web_logs"
FICHIER_ETAT = RACINE / "lake" / "_etat" / "watermarks.json"
CLE_ETAT = "logs_fichiers_traites"

# L'expression régulière qui décrit une ligne de log valide.
# Chaque (?P<nom>...) capture un morceau de la ligne et lui donne un nom.
LIGNE_LOG = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ '                                       # 154.210.254.76 - -
    r'\[(?P<horodatage>[^\]]+)\] '                                 # [01/Jan/2026:11:34:09 +0100]
    r'"(?P<methode>[A-Z]+) (?P<url>\S+) (?P<protocole>[^"]+)" '    # "GET /produits HTTP/1.1"
    r'(?P<code_http>\d{3}) (?P<taille>\d+) '                       # 200 12158
    r'"(?P<referer>[^"]*)" "(?P<user_agent>[^"]*)"$'               # "-" "Mozilla/5.0 ..."
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("extraire_logs")


def lire_fichiers_traites() -> set[str]:
    """Renvoie l'ensemble des fichiers de logs déjà traités."""
    if not FICHIER_ETAT.exists():
        return set()
    etat = json.loads(FICHIER_ETAT.read_text())
    return set(etat.get(CLE_ETAT, []))


def enregistrer_fichiers_traites(fichiers: set[str]) -> None:
    """Enregistre la liste des fichiers traités, sans effacer l'état des autres sources."""
    FICHIER_ETAT.parent.mkdir(parents=True, exist_ok=True)
    etat = json.loads(FICHIER_ETAT.read_text()) if FICHIER_ETAT.exists() else {}
    etat[CLE_ETAT] = sorted(fichiers)
    temporaire = FICHIER_ETAT.with_suffix(".tmp")
    temporaire.write_text(json.dumps(etat, indent=2))
    os.replace(temporaire, FICHIER_ETAT)


def analyser_ligne(ligne: str) -> dict | None:
    """Découpe une ligne de log en colonnes. Renvoie None si la ligne est illisible."""
    correspondance = LIGNE_LOG.match(ligne)
    if correspondance is None:
        return None
    champs = correspondance.groupdict()  # {"ip": "154.210.254.76", "url": "/produits", ...}
    champs["code_http"] = int(champs["code_http"])  # sûr : l'expression garantit 3 chiffres
    champs["taille"] = int(champs["taille"])
    return champs


def lire_fichier_logs(fichier: Path) -> tuple[list[dict], list[dict]]:
    """Lit un fichier de logs et renvoie (lignes valides, lignes rejetées)."""
    valides, rejets = [], []
    with open(fichier, encoding="utf-8", errors="replace") as f:
        for numero, ligne in enumerate(f, start=1):
            ligne = ligne.rstrip("\n")
            champs = analyser_ligne(ligne)
            if champs is None:
                rejets.append({"fichier": fichier.name, "numero_ligne": numero,
                               "raison": "format de ligne non reconnu", "ligne": ligne})
            else:
                champs["_fichier_source"] = fichier.name
                valides.append(champs)
    return valides, rejets


def main() -> None:
    deja_traites = lire_fichiers_traites()
    tous_les_fichiers = sorted(DOSSIER_SOURCE.glob("access_*.log"))
    nouveaux = [f for f in tous_les_fichiers if f.name not in deja_traites]

    log.info("%s fichiers dans la source, %s déjà traités, %s nouveaux",
             len(tous_les_fichiers), len(deja_traites), len(nouveaux))
    if not nouveaux:
        log.info("Aucun nouveau fichier de logs : rien à faire.")
        return

    # 1. LIRE et découper chaque ligne
    valides, rejets = [], []
    for fichier in nouveaux:
        v, r = lire_fichier_logs(fichier)
        valides.extend(v)
        rejets.extend(r)

    maintenant = datetime.now(UTC)
    lot = f"{maintenant:%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:6]}"
    for ligne in valides:
        ligne["_ingested_at"] = maintenant
        ligne["_source"] = "serveur_web.logs"
        ligne["_batch_id"] = lot

    # 2. ÉCRIRE les lignes valides dans le data lake
    dossier = DOSSIER_RAW / f"date={maintenant:%Y-%m-%d}"
    dossier.mkdir(parents=True, exist_ok=True)
    fichier = dossier / f"web_logs_{lot}.parquet"
    temporaire = fichier.with_suffix(".tmp")
    pq.write_table(pa.Table.from_pylist(valides), temporaire)
    os.replace(temporaire, fichier)
    log.info("%s lignes valides écrites dans %s", len(valides), fichier.relative_to(RACINE))

    # 3. METTRE DE CÔTÉ les lignes illisibles (au format JSON Lines : un objet JSON par ligne)
    if rejets:
        dossier_rejets = DOSSIER_REJETS / f"date={maintenant:%Y-%m-%d}"
        dossier_rejets.mkdir(parents=True, exist_ok=True)
        fichier_rejets = dossier_rejets / f"rejets_{lot}.jsonl"
        with open(fichier_rejets, "w", encoding="utf-8") as f:
            for rejet in rejets:
                f.write(json.dumps(rejet, ensure_ascii=False) + "\n")
        taux = 100 * len(rejets) / (len(valides) + len(rejets))
        log.warning("%s lignes rejetées (%.1f %%) mises de côté dans %s",
                    len(rejets), taux, fichier_rejets.relative_to(RACINE))

    # 4. SEULEMENT APRÈS : noter ces fichiers comme traités
    enregistrer_fichiers_traites(deja_traites | {f.name for f in nouveaux})


if __name__ == "__main__":
    main()
