"""
Job de Spark Structured Streaming: Kafka -> Bronze (Delta Lake).

Lee continuamente el topico de eventos CDC de ventas y los persiste tal cual
llegan (append-only, sin transformar) en la capa Bronze, en formato Delta,
con checkpointing para garantizar procesamiento exactly-once ante reinicios.

Uso:
    spark-submit \
        --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        stream_kafka_to_bronze.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ventas_cdc")
LAKEHOUSE_PATH = os.environ.get("LAKEHOUSE_PATH", "/opt/data_lakehouse")

BRONZE_PATH = f"{LAKEHOUSE_PATH}/bronze/ventas"
CHECKPOINT_PATH = f"{LAKEHOUSE_PATH}/checkpoints/bronze/ventas"

EVENT_SCHEMA = StructType(
    [
        StructField("tx_id", StringType(), True),
        StructField("cliente_id", StringType(), True),
        StructField("producto", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("precio", DoubleType(), True),
        StructField("cantidad", IntegerType(), True),
        StructField("tipo_operacion", StringType(), True),
        StructField("timestamp", StringType(), True),
    ]
)


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("stream_kafka_to_bronze")
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

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_value", "timestamp AS kafka_timestamp")
        .withColumn("data", from_json(col("json_value"), EVENT_SCHEMA))
        .select("data.*", "kafka_timestamp")
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    query = (
        parsed_stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start(BRONZE_PATH)
    )

    print(f"[stream_kafka_to_bronze] escribiendo en {BRONZE_PATH} (checkpoint: {CHECKPOINT_PATH})")
    query.awaitTermination()


if __name__ == "__main__":
    main()
