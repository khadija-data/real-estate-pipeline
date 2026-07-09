from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path("/opt/airflow")
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
DBT_PROFILES_DIR = DBT_PROJECT_DIR
BRONZE_SCRIPT = PROJECT_ROOT / "bronze" / "load_bronze.py"

default_args = {
    "owner": "khadija",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dbt_env = {
    "DBT_PROFILES_DIR": str(DBT_PROFILES_DIR),
}


with DAG(
    dag_id="real_estate_full_pipeline",
    description="Run the full real estate pipeline: bronze load, silver dbt models, and gold dbt models.",
    default_args=default_args,
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["real-estate", "bronze", "silver", "gold", "dbt", "snowflake"],
) as dag:
    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"cd {PROJECT_ROOT} && python {BRONZE_SCRIPT}",
    )

    run_silver = BashOperator(
        task_id="run_silver_models",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select silver",
        env=dbt_env,
        append_env=True,
    )

    test_silver = BashOperator(
        task_id="test_silver_models",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select silver",
        env=dbt_env,
        append_env=True,
    )

    run_gold = BashOperator(
        task_id="run_gold_models",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select gold",
        env=dbt_env,
        append_env=True,
    )

    test_gold = BashOperator(
        task_id="test_gold_models",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --select gold",
        env=dbt_env,
        append_env=True,
    )

    load_bronze >> run_silver >> test_silver >> run_gold >> test_gold
