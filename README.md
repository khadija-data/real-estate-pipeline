
# Real Estate Pipeline — Data Warehouse Immobilier Mondial

> Pipeline de données de bout en bout : **CSV brut → Snowflake (Bronze/Silver/Gold) → dbt → Airflow → Power BI**
> Projet de groupe — [INT-Maroc] DATA Analyst

[![dbt](https://img.shields.io/badge/dbt-1.11.6-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.10-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Snowflake](<https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake&logoColor=white>)](https://www.snowflake.com/)
[![Power BI](<https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?logo=powerbi&logoColor=black>)](https://powerbi.microsoft.com/)

---

## Sommaire

- [Contexte du projet](#contexte-du-projet)
- [Architecture](#architecture)
- [Stack technique](#stack-technique)
- [Structure du dépôt](#structure-du-dépôt)
- [Données source &amp; qualité](#données-source--qualité)
- [Modèle de données](#modèle-de-données)
- [Prérequis](#prérequis)
- [Installation &amp; configuration](#installation--configuration)
- [Exécution du pipeline](#exécution-du-pipeline)
- [Tests dbt](#tests-dbt)
- [Dashboard Power BI](#dashboard-power-bi)
- [Workflow Git &amp; organisation d&#39;équipe](#workflow-git--organisation-déquipe)
- [Limitations connues / TODO](#limitations-connues--todo)
- [Équipe](#équipe)

---

## Contexte du projet

Ce projet implémente un **Data Warehouse immobilier** couvrant plusieurs pays, construit selon une **architecture Medallion** (Bronze → Silver → Gold) dans **Snowflake**, orchestré par **Apache Airflow**, transformé avec **dbt**, et exposé via un **dashboard Power BI** connecté directement à la couche Gold.

À partir d'un fichier CSV brut contenant volontairement des problèmes de qualité (valeurs manquantes, doublons, types incohérents, formats de date multiples, casse incohérente...), le pipeline produit un schéma dimensionnel prêt pour l'analyse décisionnelle.

## Architecture

```
                ┌──────────────────────┐
                │  real_estate_raw.csv │
                └──────────┬───────────┘
                           │  load_bronze.py
                           ▼
   ┌────────────────────────────────────────────┐
   │  SNOWFLAKE — BRONZE                         │
   │  REAL_ESTATE_DB.BRONZE.REAL_ESTATE_BRONZE   │
   │  (données brutes + colonne _loaded_at)      │
   └──────────────────────┬───────────────────────┘
                           │  dbt run --select silver
                           ▼
   ┌────────────────────────────────────────────┐
   │  SNOWFLAKE — SILVER                         │
   │  silver_real_estate                         │
   │  (nettoyage, dédup, valeurs dérivées)       │
   └──────────────────────┬───────────────────────┘
                           │  dbt run --select gold
                           ▼
   ┌────────────────────────────────────────────┐
   │  SNOWFLAKE — GOLD (Star Schema)             │
   │  fact_listings + dim_location + dim_property│
   │  + dim_date                                 │
   └──────────────────────┬───────────────────────┘
                           │  connexion directe
                           ▼
                ┌──────────────────────┐
                │     Power BI (.pbix) │
                │  3 pages d'analyse   │
                └──────────────────────┘

Orchestration : Apache Airflow (DAG real_estate_full_pipeline)
```

## Stack technique

| Composant        | Technologie                         | Version |
| ---------------- | ----------------------------------- | ------- |
| Data Warehouse   | Snowflake                           | —      |
| Transformation   | dbt (dbt-snowflake)                 | 1.11.6  |
| Orchestration    | Apache Airflow                      | 2.10.0  |
| Ingestion        | Python (snowflake-connector-python) | 3.6.0   |
| Visualisation    | Power BI Desktop                    | —      |
| Conteneurisation | Docker / Docker Compose             | —      |

## Structure du dépôt

```
real-estate-pipeline/
├── airflow/
│   └── dags/
│       └── real_estate_pipeline_dag.py   # DAG d'orchestration
├── data/
│   └── raw/
│       └── real_estate_raw.csv           # Données source
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── bronze/
│       │   └── load_bronze.py            # Script de chargement Bronze
│       ├── staging/
│       │   └── sources.yml               # Déclaration de la source Bronze
│       ├── silver/
│       │   └── silver_real_estate.sql    # Nettoyage & enrichissement
│       └── gold/
│           ├── dim_location.sql
│           ├── dim_property.sql
│           ├── dim_date.sql
│           ├── fact_listings.sql
│           └── schema.yml                # Tests & documentation
├── powerbi/
│   ├── report/
│   │   └── Real-Estate-Report.pbix
│   └── screenshots/
│       ├── 01-Vue Generale du Marche.jpeg
│       ├── 02-Analyse des Prix.jpeg
│       └── 03-Caracteristiques des Biens.jpeg
├── docker-compose.yml
├── Dockerfile.airflow
├── requirements.txt
├── .env.example
└── README.md
```

## Données source & qualité

Le fichier `data/raw/real_estate_raw.csv` (2 060 annonces) couvre plusieurs pays et contient volontairement les problèmes suivants, corrigés dans la couche **Silver** :

| Problème                         | Exemple                                 | Traitement Silver                                                            |
| --------------------------------- | --------------------------------------- | ---------------------------------------------------------------------------- |
| Valeurs manquantes                | `country`, `neighborhood` vides     | Imputation par mode / médiane                                               |
| Doublons                          | `listing_id` répété                | Déduplication (`ROW_NUMBER` sur `_loaded_at`)                           |
| Types incohérents                | `"150000 EUR"` au lieu de `150000`  | `REGEXP_REPLACE` + `TRY_CAST`                                            |
| Formats de date multiples         | `06/02/2020`, `2019-01-16`          | `TRY_TO_DATE` multi-format                                                 |
| Casse/espaces incohérents        | `" Villa "`, `SUBURBS`              | `TRIM` + normalisation `CASE WHEN`                                       |
| Valeurs incohérentes booléennes | `parking = "1"`, `"yes"`, `"YES"` | Conversion en`BOOLEAN`                                                     |
| Valeurs aberrantes                | Surfaces ou prix irréalistes           | Filtrage par plage plausible (`surface_m2` 15–1000 m², `price` 5k–5M) |

Colonnes dérivées ajoutées en Silver : `price_per_m2`, `property_age`.

## Modèle de données

**Schéma choisi : Star Schema** — une table de faits centrale reliée à des dimensions dénormalisées, adaptée au volume modéré de données et à la simplicité des requêtes Power BI.

- **`fact_listings`** — grain : une ligne par annonce (`listing_id`)
  - Mesures : `price`, `surface_m2`, `price_per_m2`, `num_rooms`, `num_bathrooms`, `floor`, `property_age`
  - Clés étrangères : `location_sk`, `property_sk`, `date_sk`
- **`dim_location`** — `country`, `city`, `neighborhood`
- **`dim_property`** — `property_type`, `heating_type`, `energy_rating`, `parking`
- **`dim_date`** — `full_date`, `year`, `quarter`, `month`, `month_name`

Toutes les tables Gold utilisent des **clés de substitution** (`*_sk`) générées par `ROW_NUMBER()`. Les tests dbt (`unique`, `not_null`, `relationships`) garantissent l'intégrité référentielle (voir `dbt/models/gold/schema.yml`).

## Prérequis

- Un compte **Snowflake** avec un warehouse, une base `REAL_ESTATE_DB` et les droits de création de schémas/tables
- **Docker** & **Docker Compose** (pour Airflow)
- **Python 3.11+** (pour lancer le script Bronze en dehors de Docker si besoin)
- **Power BI Desktop** avec le connecteur Snowflake
- **dbt-snowflake 1.11.6** (installé automatiquement dans l'image Airflow, ou en local via `requirements.txt`)

## Installation & configuration

1. **Cloner le dépôt**

   ```bash
   git clone https://github.com/khadija-data/real-estate-pipeline.git
   cd real-estate-pipeline
   ```
2. **Configurer les variables d'environnement**

   ```bash
   cp .env.example .env
   ```

   Renseigner dans `.env` :

   ```
   SNOWFLAKE_ACCOUNT=...
   SNOWFLAKE_USER=...
   SNOWFLAKE_PASSWORD=...
   SNOWFLAKE_DATABASE=REAL_ESTATE_DB
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   SNOWFLAKE_ROLE=ACCOUNTADMIN
   ```

   Ne jamais committer `.env` (déjà ignoré par `.gitignore`).
3. **Configurer le profil dbt**

   ```bash
   cp dbt/profiles.yml.example dbt/profiles.yml
   ```

   Compléter avec les mêmes identifiants Snowflake (ce fichier est ignoré par git).
4. **Créer les schémas Snowflake** (une seule fois, via une feuille de calcul Snowflake ou un script SQL) :

   ```sql
   CREATE DATABASE IF NOT EXISTS REAL_ESTATE_DB;
   CREATE SCHEMA IF NOT EXISTS REAL_ESTATE_DB.BRONZE;
   CREATE SCHEMA IF NOT EXISTS REAL_ESTATE_DB.SILVER;
   CREATE SCHEMA IF NOT EXISTS REAL_ESTATE_DB.GOLD;
   ```
5. **Installer les dépendances Python (optionnel, pour exécuter hors Docker)**

   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

## Exécution du pipeline

### Option A — Via Airflow (Docker, recommandé)

```bash
docker compose up --build
```

Puis ouvrir l'interface Airflow sur **http://localhost:8080**, activer et déclencher le DAG **`real_estate_full_pipeline`**.

Le DAG exécute dans l'ordre :

1. `load_bronze` — chargement du CSV brut dans `BRONZE.REAL_ESTATE_BRONZE`
2. `run_silver_models` — `dbt run --select silver`
3. `test_silver_models` — `dbt test --select silver`
4. `run_gold_models` — `dbt run --select gold`
5. `test_gold_models` — `dbt test --select gold`

Chaque tâche dispose de **2 tentatives automatiques** (`retries=2`, délai de 5 min) et ses logs sont consultables dans l'UI Airflow (onglet *Logs* de chaque task).

### Option B — En local (sans Airflow)

```bash
# 1. Chargement Bronze
python dbt/models/bronze/load_bronze.py

# 2. Modèles Silver
cd dbt
dbt run --select silver
dbt test --select silver

# 3. Modèles Gold
dbt run --select gold
dbt test --select gold
```

## Tests dbt

Les tests génériques (`unique`, `not_null`, `relationships`) sont définis dans `dbt/models/gold/schema.yml` et valident :

- L'unicité et la non-nullité des clés de substitution
- L'intégrité référentielle entre `fact_listings` et chaque dimension

```bash
cd dbt
dbt test
dbt docs generate && dbt docs serve   # documentation interactive du DAG dbt
```

## Dashboard Power BI

Fichier : `powerbi/report/Real-Estate-Report.pbix` — **connexion directe** (import mode ou DirectQuery) à `REAL_ESTATE_DB.GOLD`, sans passage par un fichier CSV intermédiaire.

### Page 1 — Vue Générale du Marché

KPI (nombre d'annonces, prix moyen, surface moyenne), répartition des annonces par pays et par type de bien, filtre pays global appliqué aux 3 pages.

![Vue Générale du Marché](<powerbi/screenshots/01-Vue%20Generale%20du%20Marche.jpeg>)

### Page 2 — Analyse des Prix

Prix moyen par pays/ville, prix médian au m² par type de bien, distribution des prix, évolution temporelle via `dim_date`, filtres prix & type de bien.

![Analyse des Prix](<powerbi/screenshots/02-Analyse%20des%20Prix.jpeg>)

### Page 3 — Caractéristiques des Biens

Distribution des surfaces par type de bien, répartition des classes énergétiques, âge moyen par pays, part des biens avec parking, tableau récapitulatif par ville, filtre condition.

![Caractéristiques des Biens](<powerbi/screenshots/03-Caracteristiques%20des%20Biens.jpeg>)

## Workflow Git & organisation d'équipe

- Aucun commit direct sur `main` — chaque membre travaille sur une branche dédiée à sa couche/composant
- Toute fusion passe par une **Pull Request** revue par au moins un autre membre
- Commits atomiques et réguliers (au moins 1 par demi-journée)

| Branche                     | Responsable | Périmètre                     |
| --------------------------- | ----------- | ------------------------------- |
| `feature/bronze`          | KHADIJA     | Ingestion & couche Bronze       |
| `feature/silver`          | AICHA       | Nettoyage & modèles dbt Silver |
| `feature/gold`            | BRAHIM      | Schéma dimensionnel Gold       |
| `feature/airflow-powerbi` | AYOUB       | Orchestration & dashboard       |

## Limitations connues / TODO

Points à corriger avant la démo finale :

- [ ] `fact_listings.sql`, `dim_location.sql`, `dim_property.sql`, `dim_date.sql` référencent `ref('stg_listings')`, qui n'existe pas dans `models/staging/` — à renommer en `ref('silver_real_estate')` ou à créer un modèle de staging intermédiaire.
- [ ] `silver_real_estate.sql` lit une colonne source `load_timestamp` alors que `load_bronze.py` crée la colonne `_loaded_at` — à harmoniser.
- [ ] `load_bronze.py` pointe vers `data/real_estate_raw.csv`, alors que le fichier se trouve dans `data/raw/real_estate_raw.csv` — corriger `CSV_PATH`.
- [ ] `docker-compose.yml` monte `./dbt_project` et `./bronze`, dossiers qui n'existent pas (le dossier réel est `./dbt` et `load_bronze.py` est dans `dbt/models/bronze/`) — aligner les volumes ou déplacer les fichiers.
- [ ] `dbt/macros/generate_schema_name.sql` contient un fragment PowerShell résiduel au lieu du macro Jinja attendu — à nettoyer.
- [ ] `.gitignore` contient des marqueurs de conflit Git non résolus (`<<<<<<<`, `=======`, `>>>>>>>`) — à merger proprement.
- [ ] Le DAG Airflow ne comporte pas encore de tâche explicite de **notification de fin de pipeline** (log/alerte) demandée dans le cahier des charges.
- [ ] Ne pas committer d'identifiants Snowflake en dur dans le code (valeurs par défaut à retirer de `load_bronze.py`).

## Équipe

Projet réalisé dans le cadre du programme **[INT-Maroc] DATA Analyst**.

| Membre  | Rôle / Couche                      |
| ------- | ----------------------------------- |
| KHADIJA | Bronze & ingestion                  |
| AICHA   | Silver & qualité des données      |
| BRAHIM  | Gold & modélisation dimensionnelle |
| AYOUB   | Airflow & Power BI                  |

---

Assigné le 29/06/2026 — Deadline 10/07/2026
