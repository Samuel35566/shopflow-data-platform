"""
Générateur de données « sales » pour le projet fil rouge ShopFlow.

Usage :
    python generer_donnees.py                    # crée les sources dans ./sources/
    python generer_donnees.py --lignes 10000000  # mode volumineux (étape 5)
    python generer_donnees.py --api              # lance l'API produits sur http://localhost:8000
    python generer_donnees.py --iot              # émet un flux de capteurs (1 ligne JSON / mesure) sur stdout

Uniquement la bibliothèque standard de Python 3.9+ : rien à installer.
Les pièges sont volontaires : doublons, montants négatifs, dates futures,
emails invalides, encodage Windows, prix en texte, devises mélangées,
paiements orphelins, lignes de logs corrompues, bots, capteurs aberrants
ou muets, et une journée sans aucune commande.
"""

import argparse
import csv
import json
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

SEED = 42
DOSSIER = Path("sources")
DEBUT = datetime(2026, 1, 1)
NB_JOURS = 90
JOUR_VIDE = DEBUT + timedelta(days=47)  # anomalie à détecter (étape 4)

PRENOMS = ["Léa", "Hugo", "Chloé", "Lucas", "Inès", "Noah", "Zoé", "Gabriel", "Manon", "Raphaël", "Élodie", "Thibault"]
NOMS = ["Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand", "Lefèvre", "Mercier", "Girard", "Faure"]
PAYS = ["France", "france", "FR", "Belgique", "Suisse", "Espagne", "Allemagne", ""]
CATEGORIES = ["Running", "running", "RUNNING ", "Vélo", "Velo", "Fitness", "Natation", "Randonnée", "rando"]
MARQUES = ["Kalenji", "Btwin", "Domyos", "Nabaiji", "Quechua", "Asics", "Nike"]
STATUTS = ["paid", "shipped", "delivered", "cancelled", "refunded", "PAID"]
DEVISES = {"EUR": 1.0, "USD": 1.08, "CHF": 0.95, "GBP": 0.86}
ENTREPOTS = ["LYON-01", "LILLE-02", "NANTES-03"]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0) Safari/604.1",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) Firefox/121.0",
    "Googlebot/2.1 (+http://www.google.com/bot.html)",
    "AhrefsBot/7.0",
]


# ---------------------------------------------------------------- produits
def generer_produits(n=300):
    rnd = random.Random(SEED)
    produits = []
    for i in range(1, n + 1):
        prix = round(rnd.uniform(5, 400), 2)
        # Piège : le prix est parfois un nombre, parfois un texte à la française
        prix_affiche = prix if rnd.random() < 0.6 else f"{prix:.2f}".replace(".", ",") + " €"
        produits.append({
            "id": i,
            "name": f"{rnd.choice(['Chaussure', 'Gourde', 'Tapis', 'Casque', 'Maillot', 'Sac', 'Montre'])} {rnd.choice(MARQUES)} {i}",
            "category": rnd.choice(CATEGORIES),
            "brand": rnd.choice(MARQUES) if rnd.random() > 0.05 else None,
            "price": prix_affiche,
            "updated_at": (DEBUT + timedelta(days=rnd.randint(0, NB_JOURS))).isoformat(),
        })
    return produits


# ---------------------------------------------------------------- clients
def generer_clients(dossier, n=2000):
    rnd = random.Random(SEED + 1)
    chemin = dossier / "clients.csv"
    # Piège : fichier exporté en cp1252 (encodage Windows), pas en UTF-8
    with open(chemin, "w", newline="", encoding="cp1252", errors="replace") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["ID Client", "Prénom", "Nom", "E-mail", "Téléphone", "Pays", "Date inscription", "Segment"])
        for i in range(1, n + 1):
            prenom, nom = rnd.choice(PRENOMS), rnd.choice(NOMS)
            email = f"{prenom}.{nom}{i}@exemple.fr".lower()
            r = rnd.random()
            if r < 0.04:
                email = email.replace("@", "")          # email invalide
            elif r < 0.08:
                email = email.upper()                   # casse incohérente
            elif r < 0.10:
                email = ""                              # champ vide
            date = DEBUT - timedelta(days=rnd.randint(0, 1500))
            fmt = rnd.choice(["%Y-%m-%d", "%d/%m/%Y"])  # formats de date mélangés
            w.writerow([i, prenom, nom, email, f"+33 6 {rnd.randint(10000000, 99999999)}",
                        rnd.choice(PAYS), date.strftime(fmt), rnd.choice(["B2C", "B2B", "b2c", ""])])
    return n


# ---------------------------------------------------------------- commandes
def generer_commandes(dossier, nb_commandes, nb_clients, nb_produits):
    rnd = random.Random(SEED + 2)
    chemin = dossier / "shop.db"
    if chemin.exists():
        chemin.unlink()
    con = sqlite3.connect(chemin)
    con.executescript("""
        CREATE TABLE orders (
            order_id INTEGER, customer_id INTEGER, order_date TEXT, status TEXT,
            total_amount REAL, currency TEXT, warehouse TEXT, updated_at TEXT
        );
        CREATE TABLE order_items (
            order_id INTEGER, line_number INTEGER, product_id INTEGER,
            quantity INTEGER, unit_price REAL, discount REAL
        );
    """)
    lots_cmd, lots_lignes, commandes_payees = [], [], []
    for oid in range(1, nb_commandes + 1):
        jour = DEBUT + timedelta(days=rnd.randint(0, NB_JOURS - 1), seconds=rnd.randint(0, 86399))
        if jour.date() == JOUR_VIDE.date():
            jour += timedelta(days=1)  # aucune commande ce jour-là
        lignes, total = [], 0.0
        for ln in range(1, rnd.randint(1, 4) + 1):
            qte = rnd.randint(1, 3)
            pu = round(rnd.uniform(5, 400), 2)
            remise = rnd.choice([0, 0, 0, 0.1, 0.2])
            total += qte * pu * (1 - remise)
            lignes.append((oid, ln, rnd.randint(1, nb_produits + 5), qte, pu, remise))  # +5 : produits inexistants
        statut = rnd.choice(STATUTS)
        total = round(total, 2)
        if rnd.random() < 0.01:
            total = -total                                    # montant négatif
        if rnd.random() < 0.005:
            jour = datetime(2030, 1, 1)                        # date dans le futur
        devise = "EUR" if rnd.random() < 0.85 else rnd.choice(list(DEVISES))
        maj = jour + timedelta(hours=rnd.randint(0, 72))
        cmd = (oid, rnd.randint(1, nb_clients + 20), jour.isoformat(sep=" "), statut,
               total, devise, rnd.choice(ENTREPOTS + [None]), maj.isoformat(sep=" "))
        lots_cmd.append(cmd)
        if rnd.random() < 0.03:                                # doublon (une mise à jour plus récente)
            lots_cmd.append(cmd[:3] + ("shipped",) + cmd[4:7] + ((maj + timedelta(days=2)).isoformat(sep=" "),))
        lots_lignes.extend(lignes)
        if statut.lower() != "cancelled" and len(commandes_payees) < 200000:
            commandes_payees.append((oid, jour, abs(total), devise))
        if len(lots_cmd) >= 50000:
            con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", lots_cmd)
            con.executemany("INSERT INTO order_items VALUES (?,?,?,?,?,?)", lots_lignes)
            lots_cmd, lots_lignes = [], []
            print(f"  commandes : {oid:,} / {nb_commandes:,}", file=sys.stderr)
    con.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?,?)", lots_cmd)
    con.executemany("INSERT INTO order_items VALUES (?,?,?,?,?,?)", lots_lignes)
    con.commit()
    con.close()
    return commandes_payees


# ---------------------------------------------------------------- paiements
def generer_paiements(dossier, commandes_payees):
    rnd = random.Random(SEED + 3)
    rep = dossier / "paiements"
    rep.mkdir(exist_ok=True)
    par_jour = {}
    for oid, jour, montant, devise in commandes_payees:
        par_jour.setdefault(jour.date(), []).append({
            "id": f"pi_{rnd.getrandbits(48):012x}",
            "object": "payment_intent",
            "amount": int(round(montant * DEVISES.get(devise, 1) * 100)),  # en centimes, façon Stripe
            "currency": devise.lower(),
            "created": int(jour.timestamp()),
            "metadata": {"order_id": str(oid)},
            "status": "succeeded" if rnd.random() > 0.03 else "failed",
        })
    for jour, paiements in par_jour.items():
        for _ in range(rnd.randint(0, 3)):                    # paiements orphelins
            p = dict(rnd.choice(paiements))
            p["id"] = f"pi_{rnd.getrandbits(48):012x}"
            p["metadata"] = {"order_id": str(10**9 + rnd.randint(0, 10**6))}
            paiements.append(p)
        with open(rep / f"paiements_{jour.isoformat()}.json", "w", encoding="utf-8") as f:
            json.dump({"object": "list", "data": paiements, "has_more": False}, f, ensure_ascii=False)
    return len(par_jour)


# ---------------------------------------------------------------- logs
def generer_logs(dossier, nb_lignes=200000):
    rnd = random.Random(SEED + 4)
    rep = dossier / "logs"
    rep.mkdir(exist_ok=True)
    chemins = ["/", "/produits", "/produit/{}", "/panier", "/commande", "/compte", "/recherche?q=velo"]
    fichiers = {}
    for _ in range(nb_lignes):
        t = DEBUT + timedelta(seconds=rnd.randint(0, NB_JOURS * 86400 - 1))
        cle = t.strftime("%Y-%m-%d")
        if cle not in fichiers:
            fichiers[cle] = open(rep / f"access_{cle}.log", "w", encoding="utf-8")
        ip = f"{rnd.randint(1, 223)}.{rnd.randint(0, 255)}.{rnd.randint(0, 255)}.{rnd.randint(1, 254)}"
        chemin = rnd.choice(chemins).format(rnd.randint(1, 300))
        code = rnd.choices([200, 301, 404, 500], weights=[90, 4, 5, 1])[0]
        ligne = (f'{ip} - - [{t.strftime("%d/%b/%Y:%H:%M:%S +0100")}] "GET {chemin} HTTP/1.1" '
                 f'{code} {rnd.randint(200, 50000)} "-" "{rnd.choice(USER_AGENTS)}"')
        if rnd.random() < 0.01:
            ligne = ligne[: rnd.randint(5, len(ligne) - 5)]   # ligne tronquée
        elif rnd.random() < 0.005:
            ligne = "\x00\x00###ERREUR DISQUE###"            # ligne corrompue
        fichiers[cle].write(ligne + "\n")
    for f in fichiers.values():
        f.close()
    return len(fichiers)


# ---------------------------------------------------------------- API produits
def lancer_api(port=8000, taille_page=50):
    produits = generer_produits()
    rnd = random.Random()

    class Gestionnaire(BaseHTTPRequestHandler):
        def do_GET(self):
            url = urlparse(self.path)
            if url.path != "/products":
                return self._repondre(404, {"error": "not_found"})
            r = rnd.random()
            if r < 0.08:
                return self._repondre(429, {"error": "too_many_requests"}, {"Retry-After": "1"})
            if r < 0.12:
                return self._repondre(500, {"error": "internal_error"})
            page = int(parse_qs(url.query).get("page", ["1"])[0])
            debut = (page - 1) * taille_page
            lot = produits[debut: debut + taille_page]
            suivante = page + 1 if debut + taille_page < len(produits) else None
            time.sleep(rnd.uniform(0.02, 0.3))
            self._repondre(200, {"data": lot, "page": page, "next_page": suivante, "total": len(produits)})

        def _repondre(self, code, corps, entetes=None):
            data = json.dumps(corps, ensure_ascii=False).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            for k, v in (entetes or {}).items():
                self.send_header(k, v)
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *args):
            pass

    print(f"API produits sur http://localhost:{port}/products?page=1  (Ctrl+C pour arrêter)")
    HTTPServer(("localhost", port), Gestionnaire).serve_forever()


# ---------------------------------------------------------------- flux IoT
def flux_iot(intervalle=0.2):
    rnd = random.Random()
    capteurs = [f"{e}-T{i}" for e in ENTREPOTS for i in range(1, 5)]
    muet = None
    while True:
        if muet is None and rnd.random() < 0.002:
            muet = (rnd.choice(capteurs), time.time() + 180)  # capteur muet 3 minutes
        if muet and time.time() > muet[1]:
            muet = None
        capteur = rnd.choice(capteurs)
        if muet and capteur == muet[0]:
            continue
        temp = round(rnd.gauss(19, 1.5), 2)
        if rnd.random() < 0.01:
            temp = rnd.choice([-999.0, 85.0, None])           # valeurs aberrantes
        mesure = {
            "sensor_id": capteur,
            "warehouse": capteur.split("-T")[0],
            "ts": datetime.now().isoformat(timespec="milliseconds"),
            "temperature_c": temp,
            "stock_level": rnd.randint(0, 500),
        }
        print(json.dumps(mesure), flush=True)
        time.sleep(intervalle)


# ---------------------------------------------------------------- main
def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--lignes", type=int, default=50000, help="nombre de commandes (défaut 50 000)")
    p.add_argument("--api", action="store_true", help="lancer l'API produits")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--iot", action="store_true", help="émettre le flux de capteurs IoT sur stdout")
    args = p.parse_args()

    if args.api:
        return lancer_api(args.port)
    if args.iot:
        return flux_iot()

    DOSSIER.mkdir(exist_ok=True)
    nb_produits = len(generer_produits())
    print("Génération des clients…", file=sys.stderr)
    nb_clients = generer_clients(DOSSIER)
    print("Génération des commandes…", file=sys.stderr)
    payees = generer_commandes(DOSSIER, args.lignes, nb_clients, nb_produits)
    print("Génération des paiements…", file=sys.stderr)
    nb_fichiers = generer_paiements(DOSSIER, payees)
    print("Génération des logs…", file=sys.stderr)
    nb_logs = generer_logs(DOSSIER)
    print(f"\nTerminé dans ./{DOSSIER}/ :")
    print(f"  shop.db            {args.lignes:,} commandes (tables orders et order_items)")
    print(f"  clients.csv        {nb_clients:,} clients (encodage cp1252, séparateur ;)")
    print(f"  paiements/         {nb_fichiers} fichiers JSON quotidiens")
    print(f"  logs/              {nb_logs} fichiers de logs d'accès")
    print("  API produits       python generer_donnees.py --api")
    print("  Flux IoT           python generer_donnees.py --iot")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, BrokenPipeError):
        pass
