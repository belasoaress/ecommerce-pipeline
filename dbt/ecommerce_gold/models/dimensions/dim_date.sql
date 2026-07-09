-- models/dimensions/dim_date.sql
-- Dimensão de datas: gera uma linha por dia no intervalo dos pedidos.
-- Usa COALESCE para garantir que funciona mesmo com Silver vazia (primeira run).

with date_spine as (

    select
        generate_series(
            COALESCE(
                (select date_trunc('day', min(order_ts)) from {{ source('silver', 'orders_clean') }}),
                current_date
            ),
            COALESCE(
                (select date_trunc('day', max(order_ts)) from {{ source('silver', 'orders_clean') }}),
                current_date
            ),
            interval '1 day'
        )::date as date_day

),

enriched as (

    select
        date_day,
        to_char(date_day, 'YYYYMMDD')::int          as date_id,
        extract(year     from date_day)::int         as year,
        extract(month    from date_day)::int         as month,
        extract(day      from date_day)::int         as day,
        extract(week     from date_day)::int         as week_of_year,
        extract(quarter  from date_day)::int         as quarter,
        extract(dow      from date_day)::int         as day_of_week,
        to_char(date_day, 'Month')                   as month_name,
        to_char(date_day, 'Day')                     as day_name,
        to_char(date_day, 'YYYY-MM')                 as year_month,
        case when extract(dow from date_day) in (0, 6)
             then true else false end                as is_weekend,
        case
            when extract(month from date_day) between 1  and 2  then 'Bim1'
            when extract(month from date_day) between 3  and 4  then 'Bim2'
            when extract(month from date_day) between 5  and 6  then 'Bim3'
            when extract(month from date_day) between 7  and 8  then 'Bim4'
            when extract(month from date_day) between 9  and 10 then 'Bim5'
            else                                                      'Bim6'
        end                                          as bimonth
    from date_spine

)

select
    date_id,
    date_day,
    year,
    month,
    day,
    week_of_year,
    quarter,
    day_of_week,
    trim(month_name)    as month_name,
    trim(day_name)      as day_name,
    year_month,
    is_weekend,
    bimonth,
    current_timestamp   as dbt_updated_at
from enriched
order by date_day