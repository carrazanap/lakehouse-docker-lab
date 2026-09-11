-- Gold: KPIs de ventas por hora. Complementa a gold_kpi_ventas (grano diario):
-- en una sesion de clase de pocas horas, agrupar por dia da un solo punto y
-- no muestra nada; por hora si se ve la evolucion en vivo.

with ventas_enriquecidas as (

    select
        *,
        date_trunc('hour', venta_timestamp)  as hora,
        precio * cantidad                    as monto_total
    from {{ ref('stg_silver_ventas') }}

)

select
    hora,
    count(distinct tx_id)                          as cantidad_transacciones,
    count(distinct cliente_id)                      as clientes_unicos,
    sum(cantidad)                                    as unidades_vendidas,
    round(sum(monto_total), 2)                       as ingresos_totales,
    round(sum(monto_total) / count(distinct tx_id), 2) as ticket_promedio
from ventas_enriquecidas
group by hora
order by hora
