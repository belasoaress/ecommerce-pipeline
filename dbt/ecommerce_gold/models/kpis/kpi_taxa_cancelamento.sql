-- models/kpis/kpi_taxa_cancelamento.sql
-- Responde: onde e o que mais cancela?
-- Granularidade: categoria × região × dia

with base as (

    select
        f.order_ts::date                                        as data,
        p.category,
        f.region,
        f.payment_method,

        count(distinct f.order_id)                              as total_pedidos,
        count(distinct case when f.status = 'completed'
              then f.order_id end)                              as pedidos_concluidos,
        count(distinct case when f.status = 'cancelled'
              then f.order_id end)                              as pedidos_cancelados,
        count(distinct case when f.status = 'pending'
              then f.order_id end)                              as pedidos_pendentes,

        round(sum(case when f.status = 'cancelled'
              then f.total_value else 0 end)::numeric, 2)       as receita_perdida,
        round(sum(f.total_value)::numeric, 2)                   as receita_potencial

    from {{ ref('fact_orders') }} f
    join {{ ref('dim_product') }} p on f.product_sk = p.product_sk
    group by f.order_ts::date, p.category, f.region, f.payment_method

)

select
    data,
    category,
    region,
    payment_method,
    total_pedidos,
    pedidos_concluidos,
    pedidos_cancelados,
    pedidos_pendentes,
    receita_perdida,
    receita_potencial,

    -- Taxa de cancelamento (%)
    round(
        pedidos_cancelados::numeric / nullif(total_pedidos, 0) * 100,
        1
    )                                                           as taxa_cancelamento_pct,

    -- Receita efetivamente realizada
    round(receita_potencial - receita_perdida, 2)               as receita_realizada,

    -- Percentual de receita perdida
    round(
        receita_perdida / nullif(receita_potencial, 0) * 100,
        1
    )                                                           as pct_receita_perdida,

    current_timestamp                                           as dbt_updated_at

from base
order by data desc, taxa_cancelamento_pct desc