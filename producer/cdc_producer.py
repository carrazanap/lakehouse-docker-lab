"""
Productor CDC sintético para el laboratorio Medallion.

Genera eventos de "ventas" en formato JSON emulando un feed de Change Data
Capture (INSERT / UPDATE / DELETE) y los publica en un tópico de Kafka.
Pensado para correr indefinidamente como contenedor autónomo.
"""
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(levelname)s %(message)s",
)
log = logging.getLogger("cdc_producer")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "ventas_cdc")
INTERVAL_SECONDS = float(os.environ.get("PRODUCER_INTERVAL_SECONDS", "1"))
MAX_EVENTS = int(os.environ.get("PRODUCER_MAX_EVENTS", "0"))  # 0 = infinito

fake = Faker("es_AR")

PRODUCTOS = [
    ("SKU-001", "Notebook Pro 14", 950000.0),
    ("SKU-002", "Mouse Inalambrico", 15000.0),
    ("SKU-003", "Teclado Mecanico", 45000.0),
    ("SKU-004", "Monitor 27 4K", 380000.0),
    ("SKU-005", "Auriculares BT", 60000.0),
    ("SKU-006", "Webcam HD", 28000.0),
    ("SKU-007", "SSD NVMe 1TB", 90000.0),
    ("SKU-008", "Silla Ergonomica", 210000.0),
    ("SKU-009", "Hub USB-C", 22000.0),
    ("SKU-010", "Impresora Laser", 175000.0),
]

# Pool de tx_id "vivos" para poder emitir UPDATE/DELETE sobre ventas ya emitidas,
# tal como ocurriría con un feed CDC real leyendo un log de transacciones.
_live_tx_ids: list[str] = []
_MAX_LIVE_TX = 500


def connect_producer() -> KafkaProducer:
    retries = 0
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                linger_ms=50,
            )
            log.info("Conectado a Kafka en %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            retries += 1
            wait = min(30, 2 * retries)
            log.warning("Kafka no disponible todavia, reintentando en %ss...", wait)
            time.sleep(wait)


def build_event() -> dict:
    """Arma un evento de venta, decidiendo si es INSERT, UPDATE o DELETE."""
    tipo_operacion = "INSERT"
    tx_id = str(uuid.uuid4())

    if _live_tx_ids and random.random() < 0.35:
        tipo_operacion = random.choice(["UPDATE", "DELETE"])
        tx_id = random.choice(_live_tx_ids)

    sku, nombre_producto, precio_base = random.choice(PRODUCTOS)
    # Pequeña variacion de precio para simular descuentos/ajustes en UPDATE
    precio = round(precio_base * random.uniform(0.9, 1.05), 2)
    cantidad = random.randint(1, 5)

    event = {
        "tx_id": tx_id,
        "cliente_id": f"CLI-{random.randint(1000, 9999)}",
        "producto": nombre_producto,
        "sku": sku,
        "precio": precio,
        "cantidad": cantidad,
        "tipo_operacion": tipo_operacion,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if tipo_operacion == "INSERT":
        _live_tx_ids.append(tx_id)
        if len(_live_tx_ids) > _MAX_LIVE_TX:
            _live_tx_ids.pop(0)
    elif tipo_operacion == "DELETE" and tx_id in _live_tx_ids:
        _live_tx_ids.remove(tx_id)

    return event


def main() -> None:
    producer = connect_producer()
    log.info(
        "Publicando en topico '%s' cada %ss (max_events=%s)",
        KAFKA_TOPIC,
        INTERVAL_SECONDS,
        MAX_EVENTS or "infinito",
    )
    emitted = 0
    try:
        while True:
            event = build_event()
            producer.send(KAFKA_TOPIC, key=event["tx_id"], value=event)
            emitted += 1
            if emitted % 25 == 0:
                producer.flush()
                log.info("Eventos emitidos: %s (ultimo: %s)", emitted, event["tipo_operacion"])
            if MAX_EVENTS and emitted >= MAX_EVENTS:
                break
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        log.info("Interrumpido por el usuario")
    finally:
        producer.flush()
        producer.close()
        log.info("Productor detenido. Total eventos emitidos: %s", emitted)


if __name__ == "__main__":
    main()
