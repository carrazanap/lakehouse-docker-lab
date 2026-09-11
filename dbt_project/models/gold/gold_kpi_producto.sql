-- Gold: KPIs de ventas por producto, calculados sobre la capa Silver
-- (Delta Lake). Cada fila resume el desempeno historico de un producto.

select
    producto,
    count(distinct tx_id)                        as cantidad_transacciones,
    sum(cantidad)                                  as unidades_vendidas,
    round(sum(precio * cantidad), 2)               as ingresos_totales,
    round(avg(precio), 2)                          as precio_promedio
from {{ ref('stg_silver_ventas') }}
group by producto
order by ingresos_totales desc
