"""
Job batch en PySpark: Bronze -> Silver (Delta Lake).

Toma los eventos crudos de Bronze, limpia nulos/inconsistencias, deduplica
por clave primaria (tx_id, quedandose con el evento mas reciente) y aplica
la semantica CDC (INSERT/UPDATE via upsert, DELETE via borrado logico real)
sobre la tabla Delta Silver usando un MERGE INTO.

Uso:
    spark-submit \
        --packages io.delta:delta-spark_2.12:3.2.0 \
        batch_bronze_to_silver.py
"""
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number

LAKEHOUSE_PATH = os.environ.get("LAKEHOUSE_PATH", "/opt/data_lakehouse")
BRONZE_PATH = f"{LAKEHOUSE_PATH}/bronze/ventas"
SILVER_PATH = f"{LAKEHOUSE_PATH}/silver/ventas"

REQUIRED_COLUMNS = ["tx_id", "cliente_id", "producto", "precio", "cantidad", "timestamp"]


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("batch_bronze_to_silver")
        .master(os.environ.get("SPARK_MASTER_URL", "local[*]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        print(f"[batch_bronze_to_silver] No existe tabla Bronze en {BRONZE_PATH} todavia. Nada que hacer.")
        return

    bronze_df = spark.read.format("delta").load(BRONZE_PATH)

    # 1) Limpieza: descartar registros con nulos en columnas clave o valores invalidos
    clean_df = (
        bronze_df.dropna(subset=REQUIRED_COLUMNS)
        .filter((col("precio") > 0) & (col("cantidad") > 0))
        .filter(col("tipo_operacion").isin("INSERT", "UPDATE", "DELETE"))
    )

    # 2) Deduplicacion por clave primaria (tx_id): nos quedamos con el evento
    #    mas reciente segun el timestamp de origen (ultimo estado conocido).
    dedup_window = Window.partitionBy("tx_id").orderBy(col("timestamp").desc())
    dedup_df = (
        clean_df.withColumn("rn", row_number().over(dedup_window))
        .filter(col("rn") == 1)
        .drop("rn", "kafka_timestamp", "ingestion_timestamp")
    )

    dedup_df.cache()
    total_events = dedup_df.count()
    print(f"[batch_bronze_to_silver] eventos limpios y deduplicados a aplicar: {total_events}")

    if total_events == 0:
        print("[batch_bronze_to_silver] no hay eventos nuevos para aplicar en Silver.")
        return

    if DeltaTable.isDeltaTable(spark, SILVER_PATH):
        silver_table = DeltaTable.forPath(spark, SILVER_PATH)
        (
            silver_table.alias("t")
            .merge(dedup_df.alias("s"), "t.tx_id = s.tx_id")
            .whenMatchedDelete(condition="s.tipo_operacion = 'DELETE'")
            .whenMatchedUpdateAll(condition="s.tipo_operacion != 'DELETE'")
            .whenNotMatchedInsertAll(condition="s.tipo_operacion != 'DELETE'")
            .execute()
        )
        print(f"[batch_bronze_to_silver] MERGE aplicado sobre tabla Silver existente en {SILVER_PATH}")
    else:
        (
            dedup_df.filter(col("tipo_operacion") != "DELETE")
            .write.format("delta")
            .mode("overwrite")
            .save(SILVER_PATH)
        )
        print(f"[batch_bronze_to_silver] tabla Silver creada en {SILVER_PATH}")

    dedup_df.unpersist()


if __name__ == "__main__":
    main()
