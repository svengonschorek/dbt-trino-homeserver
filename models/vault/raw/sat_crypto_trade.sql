{{
    config(
        materialized = 'table'
    )
}}

-- select data from sources
-----------------------------------------------
with binance_trades_spot as (

    select * from {{ source('airbyte', 's3_binance_trades_spot') }}

),

binance_trades_futures as (

    select * from {{ source('airbyte', 's3_binance_trades_futures') }}

),

-- implement logic to build the model
-----------------------------------------------
base_spot as (

    select
        *,
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
        *,
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
        cast(concat(b.unique_key, '_', cast(b.r as varchar)) as varchar(255)) as fk_crypto_trade,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts,
        -- properties
        cast(b.trade_utc_at as timestamp) at time zone 'Europe/Berlin' as trade_at,
        cast('binance' as varchar(255)) as platform,
        cast('Spot' as varchar(255)) as wallet,
        cast(b.side as varchar(255)) as side,
        cast(b.pair as varchar(255)) as pair,
        cast(regexp_extract(b.amount, '(\\d+).(\\d+)', 0) as decimal(20, 8)) as trade_amount,
        cast(regexp_extract(b.amount, '[A-Z]+', 0) as varchar(10)) as trade_coin,
        cast(regexp_extract(b.fee, '(\\d+).(\\d+)', 0) as decimal(20, 8)) as fee_amount,
        cast(regexp_extract(b.fee, '[A-Z]+', 0) as varchar(10)) as fee_coin,
        cast(regexp_extract(b.executed, '(\\d+).(\\d+)', 0) as decimal(20, 8)) as executed_amount,
        cast(regexp_extract(b.executed, '[A-Z]+', 0) as varchar(10)) as executed_coin,
        null as realized_profit
    from base_spot as b

    union all

    select
        -- keys
        cast(concat(b.unique_key, '_', cast(b.r as varchar)) as varchar(255)) as fk_crypto_trade,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts,
        -- properties
        cast(b.trade_utc_at as timestamp) at time zone 'Europe/Berlin' as trade_at,
        cast('binance' as varchar(255)) as platform,
        cast('USD-M Futures' as varchar(255)) as wallet,
        cast(b.side as varchar(255)) as side,
        cast(b.symbol as varchar(255)) as pair,
        cast(regexp_extract(b.amount, '(\\d+).(\\d+)', 0) as decimal(20, 8)) as trade_amount,
        cast('UDST' as varchar(10)) as trade_coin,
        cast(regexp_extract(b.fee, '(\\d+).(\\d+)', 0) as decimal(20, 8)) as fee_amount,
        cast(regexp_extract(b.fee, '[A-Z]+', 0) as varchar(10)) as fee_coin,
        cast(regexp_extract(b.quantity, '(\\d+).(\\d+)', 0) as decimal(20, 8)) as executed_amount,
        cast(replace(b.symbol, 'USDT', '') as varchar(10)) as executed_coin,
        cast(realized_profit as decimal(20, 8)) as realized_profit
    from base_futures as b

)

select * from final
