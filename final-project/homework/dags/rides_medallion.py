"""DAG: Bronze (Spark) -> Silver -> Gold -> reconcile. ЕТАП 3 — створіть DAG за SPEC.md, розділ 5.

Контейнер Airflow має Java, pyspark, JDBC-драйвер і dbt (в окремому venv, див.
docker/Dockerfile.airflow). Проєкт змонтовано в /opt/airflow/project.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/opt/airflow/project"
DBT_BIN = "/home/airflow/dbt-venv/bin/dbt"
DBT = f"cd {PROJECT} && DBT_TARGET_PATH=/tmp/dbt-target DBT_LOG_PATH=/tmp/dbt-logs {DBT_BIN}"
DBT_DIRS = "--project-dir dbt_rides --profiles-dir dbt_rides"

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="rides_medallion",
    default_args=default_args,
    description=(
        "Medallion architecture pipeline: Bronze Spark -> Bronze Contract "
        "-> Silver -> Gold -> Reconcile"
    ),
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["dateng", "medallion", "dbt", "spark"],
) as dag:
    # 1. Запуск Spark-джобі для інкрементального завантаження Landing у Bronze
    bronze_spark = BashOperator(
        task_id="bronze_spark",
        bash_command=f"cd {PROJECT} && python3 bronze_job.py",
    )

    # 2. Контракт Bronze-шару (перевірка джерел через dbt test)
    bronze_cmd = (
        f"{DBT} test {DBT_DIRS} --select source:bronze --exclude assert_bronze_silver_reconcile"
    )
    bronze_contract = BashOperator(
        task_id="bronze_contract",
        bash_command=bronze_cmd,
    )

    # 3. Побудова Silver-шару (використовуємо selector з selectors.yml та cautious вибір)
    silver = BashOperator(
        task_id="silver",
        bash_command=f"{DBT} build {DBT_DIRS} --selector silver --indirect-selection cautious",
    )

    # 4. Побудова Gold-шару
    gold = BashOperator(
        task_id="gold",
        bash_command=f"{DBT} build {DBT_DIRS} --selector gold --indirect-selection cautious",
    )

    # 5. Міжшарова звірка (використовуємо selector з selectors.yml)
    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=f"{DBT} test {DBT_DIRS} --selector reconcile",
    )

    # Порядок виконання задач згідно з Етапом 3
    bronze_spark >> bronze_contract >> silver >> gold >> reconcile
