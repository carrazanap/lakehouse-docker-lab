#!/usr/bin/env bash
# Sube los jobs de Spark (spark/jobs/*.py) a DBFS para que puedan ser
# ejecutados por Databricks Jobs desde el DAG de Airflow en EXECUTION_MODE=databricks
# (ver DatabricksSubmitRunOperator en airflow/dags/dag_medallion_pipeline.py).
#
# Requisitos:
#   pip install databricks-cli   (o el nuevo "databricks" CLI unificado)
#   Variables de entorno DATABRICKS_HOST y DATABRICKS_TOKEN exportadas,
#   o un perfil configurado con `databricks configure --token`.
#
# Uso:
#   ./scripts/deploy_to_databricks.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JOBS_DIR="${SCRIPT_DIR}/spark/jobs"
DBFS_TARGET="dbfs:/lakehouse/jobs"

if ! command -v databricks &>/dev/null; then
  echo "El CLI 'databricks' no esta instalado. Ejecuta: pip install databricks-cli" >&2
  exit 1
fi

echo "Subiendo jobs de Spark a ${DBFS_TARGET} ..."
databricks fs mkdirs "${DBFS_TARGET}" || true

for job_file in "${JOBS_DIR}"/*.py; do
  echo " - $(basename "${job_file}")"
  databricks fs cp --overwrite "${job_file}" "${DBFS_TARGET}/$(basename "${job_file}")"
done

echo "Listo. Los scripts quedaron disponibles en ${DBFS_TARGET}/"
echo "El DAG (EXECUTION_MODE=databricks) los referencia como:"
echo "  dbfs:/lakehouse/jobs/batch_bronze_to_silver.py"
