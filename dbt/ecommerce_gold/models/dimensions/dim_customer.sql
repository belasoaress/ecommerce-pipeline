-- models/dimensions/dim_customer.sql
-- Dimensão de clientes: um registro por customer_id.
-- Inclui segmentação RFM simplificada e taxa de cancelamento.

with source as (

    select
        customer_id,
        max(customer_name)                                              as customer_name,
        max(customer_email)                                             as customer_email,
        max(region)                                                     as region,
        max(city)                                                       as city,
        count(distinct order_id)                                        as total_orders,
        round(sum(total_value)::numeric, 2)                             as total_spent,
        round(avg(total_value)::numeric, 2)                             as avg_order_value,
        min(order_ts)                                                   as first_order_at,
        max(order_ts)                                                   as last_order_at,
        count(distinct case when status = 'cancelled'
              then order_id end)                                        as cancelled_orders,
        max(loaded_at)                                                  as last_seen_at
    from {{ source('silver', 'orders_clean') }}
    where customer_id is not null
    group by customer_id

)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }}             as customer_sk,
    customer_id,
    customer_name,
    customer_email,
    region,
    city,
    -- Segmentação por volume de compras
    case
        when total_orders >= 5 then 'vip'
        when total_orders >= 2 then 'recorrente'
        else                        'novo'
    end                                                                 as customer_segment,
    total_orders,
    total_spent,
    avg_order_value,
    cancelled_orders,
    round(
        cancelled_orders::numeric / nullif(total_orders, 0) * 100, 1
    )                                                                   as cancellation_rate_pct,
    first_order_at,
    last_order_at,
    last_seen_at,
    current_timestamp                                                   as dbt_updated_at
from source