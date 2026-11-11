"""Logs web — extraction INCRÉMENTALE des fichiers de logs, découpés par expression régulière.

Lignes valides  : lake/raw/web_logs/date=AAAA-MM-JJ/web_logs_<lot>.parquet
Lignes rejetées : lake/rejets/web_logs/date=AAAA-MM-JJ/rejets_<lot>.jsonl
Lancement       : make extract-logs
"""

import json
import re
from pathlib import Path

from commun import (
    DOSSIER_RAW,
    DOSSIER_REJETS,
    DOSSIER_SOURCES,
    RACINE,
    ajouter_colonnes_techniques,
    chemin_dans_le_lac,
    ecrire_etat,
    ecrire_parquet,
    ecrire_texte_atomique,
    get_logger,
    lire_etat,
    nouveau_lot,
)

log = get_logger("logs_web")
DOSSIER_SOURCE = DOSSIER_SOURCES / "logs"
CLE_ETAT = "logs_fichiers_traites"

# Chaque (?P<nom>...) capture un morceau de la ligne et lui donne un nom.
LIGNE_LOG = re.compile(
    r'^(?P<ip>\S+) \S+ \S+ '                                       # 154.210.254.76 - -
    r'\[(?P<horodatage>[^\]]+)\] '                                 # [01/Jan/2026:11:34:09 +0100]
    r'"(?P<methode>[A-Z]+) (?P<url>\S+) (?P<protocole>[^"]+)" '    # "GET /produits HTTP/1.1"
    r'(?P<code_http>\d{3}) (?P<taille>\d+) '                       # 200 12158
    r'"(?P<referer>[^"]*)" "(?P<user_agent>[^"]*)"$'               # "-" "Mozilla/5.0 ..."
)


def analyser_ligne(ligne: str) -> dict | None:
    """Découpe une ligne de log en colonnes. Renvoie None si la ligne est illisible."""
    correspondance = LIGNE_LOG.match(ligne)
    if correspondance is None:
        return None
    champs = correspondance.groupdict()
    champs["code_http"] = int(champs["code_http"])
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
    deja_traites = set(lire_etat(CLE_ETAT, []))
    tous_les_fichiers = sorted(DOSSIER_SOURCE.glob("access_*.log"))
    nouveaux = [f for f in tous_les_fichiers if f.name not in deja_traites]
    log.info("%s fichiers dans la source, %s déjà traités, %s nouveaux",
             len(tous_les_fichiers), len(deja_traites), len(nouveaux))
    if not nouveaux:
        log.info("Aucun nouveau fichier de logs : rien à faire.")
        return

    valides, rejets = [], []
    for fichier in nouveaux:
        v, r = lire_fichier_logs(fichier)
        valides.extend(v)
        rejets.extend(r)

    maintenant, lot = nouveau_lot()
    ajouter_colonnes_techniques(valides, "serveur_web.logs", maintenant, lot)
    fichier = chemin_dans_le_lac(DOSSIER_RAW / "web_logs", maintenant, f"web_logs_{lot}.parquet")
    ecrire_parquet(valides, fichier)
    log.info("%s lignes valides écrites dans %s", len(valides), fichier.relative_to(RACINE))

    if rejets:
        fichier_rejets = chemin_dans_le_lac(DOSSIER_REJETS / "web_logs", maintenant, f"rejets_{lot}.jsonl")
        texte = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rejets)
        ecrire_texte_atomique(fichier_rejets, texte)
        taux = 100 * len(rejets) / (len(valides) + len(rejets))
        log.warning("%s lignes rejetées (%.1f %%) mises de côté dans %s",
                    len(rejets), taux, fichier_rejets.relative_to(RACINE))

    ecrire_etat(CLE_ETAT, sorted(deja_traites | {f.name for f in nouveaux}))


if __name__ == "__main__":
    main()
