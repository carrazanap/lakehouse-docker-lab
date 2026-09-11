#!/usr/bin/env bash
# Sube las tablas Delta de la capa Gold (generadas por dbt en
# dbt_project/spark-warehouse/) a un contenedor de Azure Storage
# (Blob o Data Lake Gen2), preservando la estructura de carpetas
# (parquet + _delta_log) tal cual la necesita un lector de Delta Lake
# como Power BI (ver README.md, seccion "Power BI + Azure Storage").
#
# Requisitos:
#   - azcopy instalado: https://learn.microsoft.com/azure/storage/common/storage-use-azcopy-v10
#   - Un Storage Account + contenedor ya creados en Azure
#   - Un SAS token con permiso de escritura sobre ese contenedor
#     (Portal de Azure: Storage Account -> Contenedor -> "Generate SAS")
#
# Uso:
#   AZURE_CONTAINER_SAS_URL='https://<cuenta>.dfs.core.windows.net/<contenedor>?<sas-token>' \
#     ./scripts/upload_gold_to_azure.sh [gold_kpi_ventas|gold_kpi_ventas_hora|gold_kpi_producto|all]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WAREHOUSE_DIR="${SCRIPT_DIR}/dbt_project/spark-warehouse"
TARGET="${1:-all}"

if [ -z "${AZURE_CONTAINER_SAS_URL:-}" ]; then
  echo "Falta AZURE_CONTAINER_SAS_URL (URL del contenedor + SAS token)." >&2
  echo "Ejemplo: https://micuenta.dfs.core.windows.net/lakehouse?sv=...&sig=..." >&2
  exit 1
fi

if ! command -v azcopy &>/dev/null; then
  echo "azcopy no esta instalado: https://learn.microsoft.com/azure/storage/common/storage-use-azcopy-v10" >&2
  exit 1
fi

upload_table() {
  local table_name="$1"
  local table_path="${WAREHOUSE_DIR}/${table_name}"

  if [ ! -d "${table_path}" ]; then
    echo "No existe ${table_path} todavia. Corre 'dbt run' (o el DAG de Airflow) primero." >&2
    return 1
  fi

  # Separamos la query string (SAS token) de la URL base del contenedor
  # para poder appendear "/gold/<tabla>" antes del "?".
  local base_url="${AZURE_CONTAINER_SAS_URL%%\?*}"
  local sas_query="${AZURE_CONTAINER_SAS_URL#*\?}"
  local dest_url="${base_url}/gold/${table_name}?${sas_query}"

  echo "Subiendo ${table_name} -> ${base_url}/gold/${table_name}"
  # El "/*" al final del origen es clave: copia el CONTENIDO de la carpeta
  # (parquet + _delta_log) directo en destino, sin anidarla una vez mas.
  azcopy copy "${table_path}/*" "${dest_url}" --recursive=true --overwrite=true
}

case "${TARGET}" in
  all)
    upload_table "gold_kpi_ventas"
    upload_table "gold_kpi_ventas_hora"
    upload_table "gold_kpi_producto"
    ;;
  gold_kpi_ventas|gold_kpi_ventas_hora|gold_kpi_producto)
    upload_table "${TARGET}"
    ;;
  *)
    echo "Uso: $0 [gold_kpi_ventas|gold_kpi_ventas_hora|gold_kpi_producto|all]" >&2
    exit 1
    ;;
esac

echo "Listo. En Power BI, apunta el 'Delta.Table(...)' a algo como:"
echo "  https://<cuenta>.dfs.core.windows.net/<contenedor>/gold/gold_kpi_ventas"
