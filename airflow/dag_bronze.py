from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

# =====================================================
# Paramètres
# =====================================================
default_args = {
    "owner": "khadija",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# =====================================================
# Fonction Bronze
# =====================================================
def load_bronze():
    """
    Exécute le script Python qui charge le CSV
    vers la table REAL_ESTATE_BRONZE.
    """
    subprocess.run(
        ["python", "/opt/airflow/scripts/load_to_bronze.py"],
        check=True
    )

# =====================================================
# DAG
# =====================================================
with DAG(
    dag_id="bronze_pipeline",
    description="Chargement CSV vers Snowflake Bronze",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["bronze", "snowflake"],
) as dag:
    bronze_task = PythonOperator(
        task_id="load_real_estate_bronze",
        python_callable=load_bronze,
    )
    bronze_task