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
        row_number() over (partition by symbol, opentime) as r,
        coin,
        basecurrency,
        from_unixtime(opentime / 1000) as kline_open_at,
        from_unixtime(closetime / 1000) as kline_close_at,
        piceopen as open_price,
        priceclose as close_price,
        pricehigh as high_price,
        pricelow as low_price,
        volume,
        quoteassetvolume as quote_asset_volume,
        numberoftrades as number_of_trades
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
        current_timestamp at time zone 'Europe/Berlin' as load_dts,
        -- properties
        cast(coin as varchar(20)) as coin,
        cast(basecurrency as varchar(20)) as basecurrency,
        cast(kline_open_at as timestamp) as kline_open_at,
        cast(kline_close_at as timestamp) as kline_close_at,
        cast(open_price as decimal(20, 8)) as open_price,
        cast(close_price as decimal(20, 8)) as close_price,
        cast(high_price as decimal(20, 8)) as high_price,
        cast(low_price as decimal(20, 8)) as low_price,
        cast(volume as decimal(20, 8)) as volume,
        cast(quote_asset_volume as decimal(20, 8)) quote_asset_volume,
        cast(number_of_trades as int) as number_of_trades
    from base
    where r = 1

)

select * from final
