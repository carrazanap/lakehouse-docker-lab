# Databricks notebook source
# MAGIC %md
# MAGIC # Explorar el Lakehouse Medallion (Azure Databricks)
# MAGIC
# MAGIC Version para Databricks de `notebooks/explorar_medallion.ipynb` (la que se usa
# MAGIC en el modo local). Para importarla: en tu Workspace/Repo de Databricks,
# MAGIC **Import** -> subis este archivo `.py` tal cual -> Databricks detecta el
# MAGIC encabezado `# Databricks notebook source` y lo abre como notebook normal,
# MAGIC con celdas separadas por `# COMMAND ----------`.
# MAGIC
# MAGIC Diferencias con la version local (Jupyter):
# MAGIC - **No hace falta crear una `SparkSession`**: Databricks ya te da `spark`
# MAGIC   (y `dbutils`) listos para usar en cualquier celda.
# MAGIC - Las rutas de Bronze/Silver ya no son `/opt/data_lakehouse/...` sino las que
# MAGIC   hayas usado al adaptar `batch_bronze_to_silver.py` para Databricks (ver
# MAGIC   seccion "Modo 2" del README): DBFS, un storage montado, o Unity Catalog.
# MAGIC   Se configuran abajo como *widgets* (parametros), no hay que tocar el codigo.
# MAGIC - **Gold se lee como tabla de catalogo** (`catalogo.schema.tabla`), ya no como
# MAGIC   ruta Delta directa — es lo que arma `dbt-databricks` al correr `dbt run`
# MAGIC   contra el SQL Warehouse.
# MAGIC - Se usa `display(df)` en vez de `.toPandas()` + matplotlib: es la funcion
# MAGIC   nativa de Databricks para tablas y gráficos interactivos (aparece un ícono
# MAGIC   de gráfico debajo de cada resultado, sin escribir código de ploteo).
# MAGIC - No incluye la celda de "espiar Kafka": un cluster de Databricks normalmente
# MAGIC   no tiene forma de llegar al Kafka que corre en tu Docker local, salvo que
# MAGIC   expongas el broker por internet (no recomendado) o repliques Kafka en Azure
# MAGIC   (Event Hubs con el protocolo de Kafka, por ejemplo).

# COMMAND ----------

dbutils.widgets.text("bronze_path", "dbfs:/lakehouse/bronze/ventas", "Ruta Bronze")
dbutils.widgets.text("silver_path", "dbfs:/lakehouse/silver/ventas", "Ruta Silver")
dbutils.widgets.text("gold_catalog", "hive_metastore", "Catalogo Gold")
dbutils.widgets.text("gold_schema", "gold", "Schema Gold")

BRONZE_PATH = dbutils.widgets.get("bronze_path")
SILVER_PATH = dbutils.widgets.get("silver_path")
GOLD_CATALOG = dbutils.widgets.get("gold_catalog")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")

# COMMAND ----------

# MAGIC %md ## Bronze — eventos crudos (append-only)

# COMMAND ----------

bronze_df = spark.read.format("delta").load(BRONZE_PATH)
print(f"Filas en Bronze: {bronze_df.count()}")
display(bronze_df.orderBy(bronze_df.timestamp.desc()).limit(20))

# COMMAND ----------

# MAGIC %md ## Silver — limpio, deduplicado, con upserts/deletes aplicados

# COMMAND ----------

silver_df = spark.read.format("delta").load(SILVER_PATH)
print(f"Filas en Silver: {silver_df.count()}")
display(silver_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC Historial de versiones Delta: cada fila es un commit (los `MERGE` de CDC
# MAGIC del batch de Silver, la misma API que en el modo local).

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY delta.`{SILVER_PATH}`")
    .select("version", "timestamp", "operation", "operationMetrics")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Unidades vendidas por producto — click en el ícono de gráfico debajo de la
# MAGIC tabla para visualizarlo como barras, sin escribir código de ploteo.

# COMMAND ----------

display(
    silver_df.groupBy("producto")
    .sum("cantidad")
    .withColumnRenamed("sum(cantidad)", "unidades_vendidas")
    .orderBy("unidades_vendidas", ascending=False)
)

# COMMAND ----------

# MAGIC %md ## Gold — KPIs agregados (creados por dbt-databricks)

# COMMAND ----------

gold_ventas_df = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_kpi_ventas")
display(gold_ventas_df.orderBy("fecha"))

# COMMAND ----------

gold_ventas_hora_df = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_kpi_ventas_hora")
display(gold_ventas_hora_df.orderBy("hora"))

# COMMAND ----------

gold_producto_df = spark.table(f"{GOLD_CATALOG}.{GOLD_SCHEMA}.gold_kpi_producto")
display(gold_producto_df.orderBy("ingresos_totales", ascending=False))
