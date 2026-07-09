-- models/kpis/kpi_volume_pedidos_por_hora.sql
-- Responde: em quais horários o volume de pedidos é maior?
-- Útil para: escalar infraestrutura, planejar campanhas, detectar anomalias.

with hourly as (

    select
        order_ts::date                                          as data,
        order_hour,
        order_hour_ts,
        count(distinct order_id)                                as total_pedidos,
        sum(quantity)                                           as total_unidades,
        round(sum(total_value)::numeric, 2)                     as receita_hora,
        round(avg(total_value)::numeric, 2)                     as ticket_medio_hora,
        count(distinct case when status = 'completed'
              then order_id end)                                as pedidos_concluidos,
        count(distinct case when status = 'cancelled'
              then order_id end)                                as pedidos_cancelados
    from {{ ref('fact_orders') }}
    where order_ts is not null
    group by order_ts::date, order_hour, order_hour_ts

),

with_stats as (

    select
        *,
        avg(total_pedidos) over (partition by data)             as media_pedidos_dia,
        COALESCE(stddev(total_pedidos) over (partition by data), 0) as desvio_pedidos_dia,

        case
            when order_hour between 6  and 11 then 'manha'
            when order_hour between 12 and 13 then 'almoco'
            when order_hour between 14 and 17 then 'tarde'
            when order_hour between 18 and 22 then 'noite'
            else                                   'madrugada'
        end                                                     as periodo_dia,

        rank() over (
            partition by data
            order by total_pedidos desc
        )                                                       as rank_hora_no_dia

    from hourly

)

select
    data,
    order_hour,
    order_hour_ts,
    periodo_dia,
    total_pedidos,
    total_unidades,
    receita_hora,
    ticket_medio_hora,
    pedidos_concluidos,
    pedidos_cancelados,
    round(media_pedidos_dia::numeric, 1)                        as media_pedidos_dia,
    round(desvio_pedidos_dia::numeric, 1)                       as desvio_pedidos_dia,
    case
        when total_pedidos > media_pedidos_dia + 2 * desvio_pedidos_dia
        then true else false
    end                                                         as is_anomalia,
    rank_hora_no_dia,
    current_timestamp                                           as dbt_updated_at
from with_stats
order by data desc, order_hour