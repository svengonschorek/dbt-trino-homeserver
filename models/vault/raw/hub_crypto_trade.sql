{{
    config(
        materialized = 'table'
    )
}}

-- select data from sources
-----------------------------------------------
with binance_trades_spot as (

    select * from {{ source('spark', 's3_binance_trades_spot') }}

),

binance_trades_futures as (

    select * from {{ source('spark', 's3_binance_trades_futures') }}

),

-- implement logic to build the model
-----------------------------------------------
base_spot as (

    select
        concat(
            'binance',
            '_',
            lower(side),
            '_',
            cast(to_unixtime(cast(trade_utc_at as timestamp)) as varchar),
            '_',
            lower(pair)
        ) as unique_key,
        row_number() over (
            partition by
                side,
                trade_utc_at,
                pair
            order by
                amount
        ) as r
    from binance_trades_spot

),

base_futures as (

    select
        trade_id as unique_key,
        row_number() over (
            partition by
                trade_id
            order by
                amount
        ) as r
    from binance_trades_futures

),

final as (

    select
        -- keys
        concat(unique_key, '_', cast(r as varchar)) as pk_crypto_trade,
        concat(unique_key, '_', cast(r as varchar)) as bk_crypto_trade,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts
    from base_spot

    union all

    select
        -- keys
        cast(concat(unique_key, '_', cast(r as varchar)) as varchar(255)) as pk_crypto_trade,
        cast(concat(unique_key, '_', cast(r as varchar)) as varchar(255)) as bk_crypto_trade,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts
    from base_futures

)

select * from final
