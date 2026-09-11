"""
Utilidad de inspeccion: muestra el esquema, el conteo de filas y una muestra
de una tabla Delta Lake a partir de su ruta (Bronze/Silver).

Uso:
    spark-submit /opt/spark/jobs/inspect_delta.py <ruta_delta> [n_filas]

Ejemplos:
    spark-submit /opt/spark/jobs/inspect_delta.py /opt/data_lakehouse/bronze/ventas
    spark-submit /opt/spark/jobs/inspect_delta.py /opt/data_lakehouse/silver/ventas 50
"""
import sys

from pyspark.sql import SparkSession


def main() -> None:
    if len(sys.argv) < 2:
        print("Uso: inspect_delta.py <ruta_delta> [n_filas]")
        sys.exit(1)

    path = sys.argv[1]
    n_rows = int(sys.argv[2]) if len(sys.argv) > 2 else 20

    spark = (
        SparkSession.builder.appName("inspect_delta")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(path)

    print(f"\n=== Esquema: {path} ===")
    df.printSchema()

    total = df.count()
    print(f"=== Total de filas: {total} ===\n")

    df.show(n_rows, truncate=False)

    print("=== Historial de versiones (Delta time travel) ===")
    spark.sql(f"DESCRIBE HISTORY delta.`{path}`").show(truncate=False)


if __name__ == "__main__":
    main()
