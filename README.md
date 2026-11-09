# ShopFlow — plateforme de données

Projet d'apprentissage du métier de data engineer : construire de A à Z la plateforme
de données d'une boutique en ligne fictive (collecte, lakehouse, entrepôt, streaming,
qualité, cloud, sécurité, CI/CD).

## Démarrage rapide

```bash
git clone git@github.com:Samuel35566/shopflow-data-platform.git
cd shopflow-data-platform
make install      # crée .venv/ et installe les dépendances
make data         # génère les 6 sources de données dans sources/
make test         # lance les tests
```

Tape `make` pour voir toutes les commandes.

## Prérequis

- Python 3.11 ou plus
- Git
- Docker (à partir de la phase 1)
- Sous Windows : travailler dans WSL2 (Ubuntu)

## Structure

| Dossier | Contenu | Phase |
| --- | --- | --- |
| `ingestion/` | Scripts de collecte des 6 sources | 1 |
| `spark_jobs/` | Jobs PySpark bronze → silver | 2, 5 |
| `dbt_shopflow/` | Projet dbt (couche gold, tests) | 3 |
| `airflow/dags/` | Orchestration | 4 |
| `streaming/` | Kafka, Flink, job Java | 5 |
| `infra/terraform/` | Infrastructure cloud | 7 |
| `k8s/` | Manifestes Kubernetes | 9 |
| `tests/` | Tests automatiques | toutes |
| `docs/` | Spécifications, schémas, décisions, journal | toutes |

## Avancement

- [x] Phase 0 — Poste de travail et dépôt
- [ ] Phase 1 — Collecte
- [ ] Phase 2 — PySpark et Delta Lake
- [ ] Phase 3 — dbt et BigQuery
- [ ] Phase 4 — Airflow
- [ ] Phase 5 — Kafka, Spark Streaming, Flink
- [ ] Phase 6 — Qualité et monitoring
- [ ] Phase 7 — Cloud, Terraform, coûts
- [ ] Phase 8 — Sécurité et gouvernance
- [ ] Phase 9 — CI/CD et Kubernetes
- [ ] Phase 10 — Collaboration
