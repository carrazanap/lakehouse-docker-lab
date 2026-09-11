-- Vista liviana sobre la capa Silver (Delta Lake), tipada. Existe para poder
-- colgarle tests de calidad "de verdad" (not_null/unique/accepted_values)
-- con la sintaxis normal de dbt, y para que los modelos Gold no dependan
-- directamente de la var de conexion a Silver.

select
    tx_id,
    cliente_id,
    producto,
    sku,
    cast(precio as double) as precio,
    cast(cantidad as int) as cantidad,
    tipo_operacion,
    cast(timestamp as timestamp) as venta_timestamp
from {{ var('silver_ventas_relation') }}
