# 🏠 Real Estate Data Pipeline

Pipeline de données complet pour un Data Warehouse immobilier mondial.  
Architecture Medallion (Bronze → Silver → Gold) dans Snowflake, orchestrée par Apache Airflow et transformée via dbt.

---

## 🏗️ Architecture

```
CSV brut
   ↓
🟫 BRONZE  →  Chargement raw dans Snowflake (Python)
   ↓
🥈 SILVER  →  Nettoyage et enrichissement (dbt)
   ↓
🥇 GOLD    →  Star Schema dimensionnel (dbt)
   ↓
📊 POWER BI →  Dashboard décisionnel (3 pages)
```

---

## 👥 Équipe et responsabilités

| Membre | Branche | Responsabilité |
|--------|---------|----------------|
| Brahim | `branch/gold` | Gold Layer — Star Schema dbt |
| Khadija | `branch/silver` | Silver Layer — Nettoyage dbt |
| Aicha | `branch/bronze` | Bronze Layer + DAG Airflow |
| Ayoub | `branch/powerbi-docs` | Power BI Dashboard + Documentation |

---

## 🗂️ Structure du projet

```
real-estate-pipeline/
├── README.md
├── .gitignore
├── requirements.txt
│
├── data/
│   └── real_estate_raw.csv          # Données source
│
├── bronze/
│   └── load_bronze.py               # Script chargement CSV → Snowflake
│
├── airflow/
│   └── dags/
│       └── real_estate_pipeline.py  # DAG Airflow complet
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml.example         # Template credentials
│   ├── models/
│   │   ├── silver/
│   │   │   ├── stg_listings.sql
│   │   │   └── schema.yml
│   │   └── gold/
│   │       ├── dim_location.sql
│   │       ├── dim_property.sql
│   │       ├── dim_date.sql
│   │       ├── fact_listings.sql
│   │       └── schema.yml
│   └── macros/
│       └── generate_schema_name.sql
│
└── powerbi/
    └── real_estate_dashboard.pbix
```

---

## ⚙️ Installation et configuration

### 1. Cloner le projet
```bash
git clone https://github.com/khadija-data/real-estate-pipeline.git
cd real-estate-pipeline
```

### 2. Installer les dépendances
```bash
pip install -r requirements.txt
pip install dbt-snowflake
```

### 3. Configurer les credentials Snowflake
```bash
cp dbt_project/profiles.yml.example ~/.dbt/profiles.yml
# Éditer ~/.dbt/profiles.yml avec vos credentials
```

### 4. Créer les schemas dans Snowflake
```sql
CREATE DATABASE IF NOT EXISTS REAL_ESTATE_DB;
CREATE SCHEMA IF NOT EXISTS REAL_ESTATE_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS REAL_ESTATE_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS REAL_ESTATE_DB.GOLD;
```

---

## 🚀 Lancer le pipeline

### Option 1 — Manuel (étape par étape)
```bash
# 1. Charger le CSV dans Bronze
python bronze/load_bronze.py

# 2. Exécuter les modèles dbt Silver
cd dbt_project
dbt run --select silver

# 3. Exécuter les modèles dbt Gold
dbt run --select gold

# 4. Valider les données
dbt test --select gold
```

### Option 2 —