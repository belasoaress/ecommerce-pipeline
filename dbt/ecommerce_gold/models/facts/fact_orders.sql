-- models/facts/fact_orders.sql
-- Tabela fato central do star schema.
-- Cada linha = 1 pedido, com surrogate keys para todas as dimensões.

with orders as (

    select * from {{ source('silver', 'orders_clean') }}
    where order_id     is not null
      and product_id   is not null
      and customer_id  is not null
      and order_ts     is not null

),

with_dim_keys as (

    select
        o.order_id,
        {{ dbt_utils.generate_surrogate_key(['o.product_id']) }}    as product_sk,
        {{ dbt_utils.generate_surrogate_key(['o.customer_id']) }}   as customer_sk,
        to_char(o.order_ts::date, 'YYYYMMDD')::int                  as date_id,
        o.price,
        o.quantity,
        o.total_value,
        o.payment_method,
        o.installments,
        o.status,
        o.region,
        extract(hour from o.order_ts)::int                          as order_hour,
        date_trunc('hour', o.order_ts)                              as order_hour_ts,
        o.order_ts,
        case when o.status = 'completed' then 1 else 0 end          as is_completed,
        case when o.status = 'cancelled' then 1 else 0 end          as is_cancelled,
        case when o.status = 'pending'   then 1 else 0 end          as is_pending,
        o.source_file,
        o.bronze_id,
        o.loaded_at
    from orders o

)

select
    {{ dbt_utils.generate_surrogate_key(['order_id']) }}            as order_sk,
    order_id,
    product_sk,
    customer_sk,
    date_id,
    price,
    quantity,
    total_value,
    payment_method,
    installments,
    status,
    region,
    order_hour,
    order_hour_ts,
    order_ts,
    is_completed,
    is_cancelled,
    is_pending,
    source_file,
    bronze_id,
    loaded_at,
    current_timestamp                                               as dbt_updated_at
from with_dim_keys