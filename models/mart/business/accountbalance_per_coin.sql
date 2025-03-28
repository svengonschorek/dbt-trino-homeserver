-- accountbalance per coin model
{{
    config(
        materialized = 'table'
    )
}}

-- select needed models
-----------------------------------------------
with sat_crypto_kline as (

    select * from {{ ref('sat_crypto_kline') }}

),

calc_crypto_accountbalance as (

    select * from {{ ref('calc_crypto_accountbalance') }}

),

-- add USDT to klines
-----------------------------------------------
klines as (

    select
        fk_crypto_kline,
        coin,
        basecurrency,
        kline_open_at,
        kline_close_at,
        open_price,
        close_price
    from sat_crypto_kline

    union all

    select
        concat('USDT-', cast(to_unixtime(kline_open_at) * 1000 as varchar)) as fk_crypto_kline,
        'USDT' as coin,
        'USDT' as basecurrency,
        kline_open_at,
        kline_close_at,
        1 as open_price,
        1 as close_price
    from sat_crypto_kline
    group by
        kline_open_at,
        kline_close_at

),

-- implement accountbalance calculation
-----------------------------------------------
base as (

    select distinct
        kl.coin,
        kl.basecurrency,
        kl.kline_open_at,
        kl.kline_close_at,
        kl.open_price,
        kl.close_price,
        first_value(coalesce(cca.accountbalance, 0)) over (
            partition by kl.fk_crypto_kline
            order by cca.transaction_at desc, cca.wallet
        ) as coin_balance_close
    from klines as kl

    left join calc_crypto_accountbalance as cca
        on (
            kl.coin = cca.coin
            and kl.kline_open_at <= coalesce(cca.valid_to, cast('9999-01-01' as timestamp))
            and kl.kline_close_at >= cca.valid_from
        )

),

-- build final CTE
-----------------------------------------------
final as (

    select
        coin,
        basecurrency,
        kline_open_at,
        kline_close_at,
        open_price,
        lead(coin_balance_close) over (
            partition by coin
            order by kline_open_at desc
        ) as coin_balance_open,
        lead(coin_balance_close) over (
            partition by coin
            order by kline_open_at desc
        ) * open_price as usdt_balance_open,
        close_price,
        coin_balance_close,
        close_price * coin_balance_close as usdt_balance_close
    from base

)

select * from final
