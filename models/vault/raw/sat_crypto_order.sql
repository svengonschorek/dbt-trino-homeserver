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
        *,
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
        cast(concat(b.unique_key, '_', cast(b.r as varchar)) as varchar(255)) as fk_crypto_order,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts,
        -- properties
        cast(b.order_utc_at as timestamp) at time zone 'Europe/Berlin' as order_at,
        cast('binance' as varchar(255)) as platform,
        cast('spot' as varchar(255))  as wallet,
        cast(b.side as varchar(50)) as side,
        cast(b.pair as varchar(50)) as pair,
        cast(b.type as varchar(50)) as type,
        cast(regexp_extract(b.order_amount, '([0-9]+\.?[0-9]*)', 0) as decimal(20, 8)) as order_amount,
        cast(regexp_extract(b.order_amount, '[A-Z]+', 0) as varchar(10)) as order_coin,
        cast(b.order_price as decimal(20, 8)) as order_price,
        cast(regexp_extract(b.executed, '([0-9]+\.?[0-9]*)', 0) as decimal(20, 8)) as executed_amount,
        cast(b.average_price as decimal(20, 8)) as executed_price,
        cast(regexp_extract(b.trading_total, '([0-9]+\.?[0-9]*)', 0) as decimal(20, 8)) as trade_amount,
        cast(regexp_extract(b.trading_total, '[A-Z]+', 0) as varchar(10)) as trade_coin
    from base as b

)

select * from final
