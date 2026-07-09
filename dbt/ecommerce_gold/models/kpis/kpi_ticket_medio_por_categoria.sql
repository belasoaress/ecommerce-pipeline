-- models/kpis/kpi_ticket_medio_por_categoria.sql
-- Responde: qual o ticket médio e receita por categoria?
-- Granularidade: categoria × dia

with base as (

    select
        f.order_ts::date                                        as data,
        p.category,
        p.product_name,

        count(distinct f.order_id)                              as total_pedidos,
        sum(f.quantity)                                         as unidades_vendidas,
        round(sum(f.total_value)::numeric, 2)                   as receita_total,
        round(avg(f.total_value)::numeric, 2)                   as ticket_medio,
        round(min(f.total_value)::numeric, 2)                   as ticket_minimo,
        round(max(f.total_value)::numeric, 2)                   as ticket_maximo

    from {{ ref('fact_orders') }} f
    join {{ ref('dim_product') }} p on f.product_sk = p.product_sk
    where f.status in ('completed', 'pending')   -- exclui cancelados do cálculo de receita

    group by f.order_ts::date, p.category, p.product_name

)

select
    data,
    category,
    product_name,
    total_pedidos,
    unidades_vendidas,
    receita_total,
    ticket_medio,
    ticket_minimo,
    ticket_maximo,
    -- Participação da receita dentro da categoria no dia
    round(
        receita_total / nullif(sum(receita_total) over (partition by data, category), 0) * 100,
        1
    )                                                           as pct_receita_na_categoria,
    current_timestamp                                           as dbt_updated_at

from base
order by data desc, receita_total desc