-- Vista liviana sobre la capa Bronze (Delta Lake), tal cual llega.
-- A proposito NO se limpia nada aca: Bronze es la captura cruda. Los tests
-- que se le cuelguen (ver schema.yml) van en severity=warn, para
-- MONITOREAR la calidad del feed de origen sin bloquear el pipeline.

select
    tx_id,
    cliente_id,
    producto,
    sku,
    precio,
    cantidad,
    tipo_operacion,
    timestamp
from {{ var('bronze_ventas_relation') }}
