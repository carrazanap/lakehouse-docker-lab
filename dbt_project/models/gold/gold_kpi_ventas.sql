-- Gold: KPIs diarios de ventas, calculados sobre la capa Silver ya
-- limpia/deduplicada (Delta Lake). Cada fila es un dia calendario.

with ventas_enriquecidas as (

    select
        *,
        date(venta_timestamp)        as fecha,
        precio * cantidad            as monto_total
    from {{ ref('stg_silver_ventas') }}

)

select
    fecha,
    count(distinct tx_id)                          as cantidad_transacciones,
    count(distinct cliente_id)                      as clientes_unicos,
    sum(cantidad)                                    as unidades_vendidas,
    round(sum(monto_total), 2)                       as ingresos_totales,
    round(sum(monto_total) / count(distinct tx_id), 2) as ticket_promedio
from ventas_enriquecidas
group by fecha
order by fecha
