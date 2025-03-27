{{
    config(
        materialized = 'table'
    )
}}

-- select data from sources
-----------------------------------------------
with binance_klines as (

    select * from {{ source('spark', 's3_binance_klines') }}

),

-- implement logic to build the model
-----------------------------------------------
base as (

    select
        concat(symbol, '-', cast(opentime as varchar)) as unique_key,
        row_number() over (partition by symbol, opentime) as r
    from binance_klines

),

-- create final CTE with the right datatype
-----------------------------------------------
final as (

    select
        -- keys
        cast(unique_key as varchar(255)) as fk_crypto_kline,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts
    from base
    where r = 1

)

select * from final
