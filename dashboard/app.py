"""
Dashboard de KPIs de la capa Gold del laboratorio Medallion.

Lee las tablas Delta que genera dbt (gold_kpi_ventas, gold_kpi_ventas_hora,
gold_kpi_producto)
directamente del disco con la libreria `deltalake` (delta-rs): no necesita
una JVM ni una SparkSession, solo leer los archivos Parquet + _delta_log.
Los datos se refrescan solos cada vez que Airflow corre `dbt run`
(por defecto, cada 15 minutos), o al tocar "Actualizar ahora".
"""
import os
import time

import pandas as pd
import plotly.express as px
import streamlit as st
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

GOLD_WAREHOUSE_PATH = os.environ.get("GOLD_WAREHOUSE_PATH", "/opt/dbt_project/spark-warehouse")
GOLD_KPI_VENTAS_PATH = f"{GOLD_WAREHOUSE_PATH}/gold_kpi_ventas"
GOLD_KPI_VENTAS_HORA_PATH = f"{GOLD_WAREHOUSE_PATH}/gold_kpi_ventas_hora"
GOLD_KPI_PRODUCTO_PATH = f"{GOLD_WAREHOUSE_PATH}/gold_kpi_producto"

REFRESH_TTL_SECONDS = 30

st.set_page_config(page_title="Gold KPIs · Medallion Lab", layout="wide", page_icon="📊")


@st.cache_data(ttl=REFRESH_TTL_SECONDS)
def load_delta_table(path: str) -> pd.DataFrame:
    dt = DeltaTable(path)
    return dt.to_pandas()


def try_load(path: str, label: str) -> pd.DataFrame | None:
    try:
        return load_delta_table(path)
    except (TableNotFoundError, FileNotFoundError):
        st.warning(
            f"Todavia no existe la tabla **{label}** en `{path}`. "
            "Corre el DAG `dag_medallion_pipeline` en Airflow (o `dbt run` a mano) "
            "para generarla."
        )
        return None


st.title("📊 KPIs de Ventas — Capa Gold")
st.caption(
    "Datos leidos directamente de las tablas Delta que produce dbt "
    "(`gold_kpi_ventas`, `gold_kpi_producto`). Se refrescan solos cada "
    f"{REFRESH_TTL_SECONDS}s si hubo una corrida nueva de dbt."
)

with st.sidebar:
    st.header("Controles")
    if st.button("🔄 Actualizar ahora"):
        st.cache_data.clear()
        st.rerun()
    st.caption(f"Ultima carga: {time.strftime('%H:%M:%S')}")
    st.divider()
    st.caption(f"Ruta Gold: `{GOLD_WAREHOUSE_PATH}`")

ventas_df = try_load(GOLD_KPI_VENTAS_PATH, "gold_kpi_ventas")
ventas_hora_df = try_load(GOLD_KPI_VENTAS_HORA_PATH, "gold_kpi_ventas_hora")
producto_df = try_load(GOLD_KPI_PRODUCTO_PATH, "gold_kpi_producto")

if ventas_df is not None and not ventas_df.empty:
    ventas_df["fecha"] = pd.to_datetime(ventas_df["fecha"])
    ventas_df = ventas_df.sort_values("fecha")

    total_ingresos = ventas_df["ingresos_totales"].sum()
    total_unidades = ventas_df["unidades_vendidas"].sum()
    total_transacciones = ventas_df["cantidad_transacciones"].sum()
    ticket_promedio = total_ingresos / total_transacciones if total_transacciones else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Ingresos totales", f"${total_ingresos:,.0f}")
    col2.metric("Unidades vendidas", f"{total_unidades:,.0f}")
    col3.metric("Transacciones", f"{total_transacciones:,.0f}")
    col4.metric("Ticket promedio", f"${ticket_promedio:,.0f}")

    with st.expander("Ver tabla gold_kpi_ventas (grano diario)"):
        st.dataframe(ventas_df, use_container_width=True)

if ventas_hora_df is not None and not ventas_hora_df.empty:
    ventas_hora_df["hora"] = pd.to_datetime(ventas_hora_df["hora"])
    ventas_hora_df = ventas_hora_df.sort_values("hora")

    st.plotly_chart(
        px.line(
            ventas_hora_df, x="hora", y="ingresos_totales", markers=True,
            title="Ingresos totales por hora",
        ),
        use_container_width=True,
    )

    with st.expander("Ver tabla gold_kpi_ventas_hora"):
        st.dataframe(ventas_hora_df, use_container_width=True)

st.divider()

if producto_df is not None and not producto_df.empty:
    producto_df = producto_df.sort_values("ingresos_totales", ascending=False)

    st.plotly_chart(
        px.bar(
            producto_df, x="producto", y="ingresos_totales",
            title="Ingresos totales por producto",
            text_auto=".2s",
        ),
        use_container_width=True,
    )

    col_a, col_b = st.columns(2)
    with col_a:
        st.plotly_chart(
            px.bar(
                producto_df, x="producto", y="unidades_vendidas",
                title="Unidades vendidas por producto",
            ),
            use_container_width=True,
        )
    with col_b:
        st.plotly_chart(
            px.bar(
                producto_df, x="producto", y="precio_promedio",
                title="Precio promedio por producto",
            ),
            use_container_width=True,
        )

    with st.expander("Ver tabla gold_kpi_producto"):
        st.dataframe(producto_df, use_container_width=True)
