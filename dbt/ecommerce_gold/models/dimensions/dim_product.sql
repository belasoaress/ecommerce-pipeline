-- models/dimensions/dim_product.sql
-- Dimensão de produtos: um registro por product_id
-- Fonte: silver.orders_clean

with source as (

    select
        product_id,
        product_name,
        category,
        avg(price)                              as avg_price,
        min(price)                              as min_price,
        max(price)                              as max_price,
        count(distinct order_id)                as total_orders,
        sum(quantity)                           as total_units_sold,
        sum(total_value)                        as total_revenue,
        max(loaded_at)                          as last_seen_at

    from {{ source('silver', 'orders_clean') }}
    where product_id is not null
    group by product_id, product_name, category

),

-- Garante unicidade por product_id
deduped as (

    select *,
        row_number() over (
            partition by product_id
            order by last_seen_at desc
        ) as rn
    from source

)

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }}  as product_sk,
    product_id,
    product_name,
    category,
    round(avg_price::numeric, 2)                            as avg_price,
    round(min_price::numeric, 2)                            as min_price,
    round(max_price::numeric, 2)                            as max_price,
    total_orders,
    total_units_sold,
    round(total_revenue::numeric, 2)                        as total_revenue,
    last_seen_at,
    current_timestamp                                       as dbt_updated_at

from deduped
where rn = 1