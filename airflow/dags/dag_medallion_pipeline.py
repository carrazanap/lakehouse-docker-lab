"""
DAG orquestador del pipeline Medallion (Bronze -> Silver -> Gold).

La ingesta continua Kafka -> Bronze corre por fuera de Airflow (contenedor
"spark-streaming" siempre activo). Este DAG se encarga de lo programado:

    1. batch_silver_refinement : limpia/deduplica Bronze y hace upsert en Silver.
    2. dbt_run                 : construye los modelos Gold (KPIs) sobre Silver.
    3. dbt_test                : corre los tests de calidad (not_null, unique).

Modo de ejecucion (variable de entorno EXECUTION_MODE, seteada en docker-compose
a partir de .env):

    - "local"      (default): el batch de Silver corre via spark-submit local[*]
                    dentro de este mismo contenedor de Airflow, y dbt usa el
                    target "local" (Spark embebido + Delta Lake sobre el volumen
                    compartido data_lakehouse).
    - "databricks": el batch de Silver se dispara como un job efimero en un
                    workspace real de Azure Databricks via
                    DatabricksSubmitRunOperator, y dbt usa el target
                    "databricks" (dbt-databricks contra un SQL Warehouse).

                    Requisitos para este modo (ver README.md):
                      * Conexion de Airflow 'databricks_default' (host + token),
                        configurable via AIRFLOW_CONN_DATABRICKS_DEFAULT.
                      * Los scripts de spark/jobs/ subidos a DBFS/Workspace
                        (ver scripts/deploy_to_databricks.sh).
"""
import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

EXECUTION_MODE = os.environ.get("EXECUTION_MODE", "local")
LAKEHOUSE_PATH = os.environ.get("LAKEHOUSE_PATH", "/opt/data_lakehouse")
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"

# PySpark y dbt viven en un virtualenv aislado del entorno principal de Airflow
# (ver airflow/Dockerfile) para evitar conflictos de dependencias con
# apache-airflow-providers-databricks. No alcanza con invocar los binarios
# por ruta absoluta: los scripts de spark-submit/dbt internamente vuelven a
# resolver "python3"/"python" via PATH, asi que si no anteponemos el bin del
# venv terminan ejecutandose con el Python (y el pyspark) del entorno
# principal de Airflow. Por eso todos los comandos exportan PATH primero.
SPARK_VENV_BIN = "/home/airflow/spark_venv/bin"
PATH_PREFIX = f'PATH="{SPARK_VENV_BIN}:$PATH"'

DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"

SPARK_SUBMIT_SILVER_CMD = (
    f"export {PATH_PREFIX} && "
    f"{SPARK_VENV_BIN}/spark-submit "
    f"--packages {DELTA_PACKAGE} "
    "--master local[*] "
    "/opt/spark_jobs/batch_bronze_to_silver.py"
)

DBT_RUN_CMD = f"export {PATH_PREFIX} && cd {DBT_PROJECT_DIR} && {SPARK_VENV_BIN}/dbt run --profiles-dir ."
DBT_TEST_CMD = f"export {PATH_PREFIX} && cd {DBT_PROJECT_DIR} && {SPARK_VENV_BIN}/dbt test --profiles-dir ."

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="dag_medallion_pipeline",
    description="Bronze->Silver (batch) y Silver->Gold (dbt) del laboratorio Medallion",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["medallion", "kafka", "spark", "delta", "dbt", EXECUTION_MODE],
)
def dag_medallion_pipeline():

    if EXECUTION_MODE == "databricks":
        from airflow.providers.databricks.operators.databricks import (
            DatabricksSubmitRunOperator,
        )

        databricks_node_type = os.environ.get("DATABRICKS_NODE_TYPE", "Standard_DS3_v2")
        databricks_spark_version = os.environ.get("DATABRICKS_SPARK_VERSION", "15.4.x-scala2.12")
        databricks_num_workers = int(os.environ.get("DATABRICKS_NUM_WORKERS", "1"))

        batch_silver_refinement = DatabricksSubmitRunOperator(
            task_id="batch_silver_refinement",
            databricks_conn_id="databricks_default",
            json={
                "run_name": "medallion_batch_bronze_to_silver",
                "new_cluster": {
                    "spark_version": databricks_spark_version,
                    "node_type_id": databricks_node_type,
                    "num_workers": databricks_num_workers,
                    "spark_conf": {
                        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                    },
                },
                "spark_python_task": {
                    "python_file": "dbfs:/lakehouse/jobs/batch_bronze_to_silver.py",
                },
            },
        )
    else:
        batch_silver_refinement = BashOperator(
            task_id="batch_silver_refinement",
            bash_command=SPARK_SUBMIT_SILVER_CMD,
            env={**os.environ, "LAKEHOUSE_PATH": LAKEHOUSE_PATH},
        )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=DBT_RUN_CMD,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=DBT_TEST_CMD,
    )

    batch_silver_refinement >> dbt_run >> dbt_test


dag_medallion_pipeline()
