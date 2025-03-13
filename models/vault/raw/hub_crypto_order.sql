{{
    config(
        materialized = 'table'
    )
}}

-- select data from sources
-----------------------------------------------
with binance_orders_spot as (

    select * from {{ source('spark', 's3_binance_orders_spot') }}

),

-- implement logic to build the model
-----------------------------------------------
base as (

    select
        concat(
            'binance',
            '_',
            lower(side),
            '_',
            cast(to_unixtime(cast(order_utc_at as timestamp)) as varchar),
            '_',
            lower(pair),
            '_',
            lower(type)
        ) as unique_key,
        row_number() over (
            partition by
                side,
                order_utc_at,
                pair,
                type
            order by
                trading_total
        ) as r
    from binance_orders_spot
    where status = 'FILLED'

),

final as (

    select
        -- keys
        cast(concat(unique_key, '_', cast(r as varchar)) as varchar(255)) as pk_crypto_order,
        cast(concat(unique_key, '_', cast(r as varchar)) as varchar(255)) as bk_crypto_order,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts
    from base

)

select * from final
