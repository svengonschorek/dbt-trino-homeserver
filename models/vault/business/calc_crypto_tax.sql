{{
    config(
        materialized = 'table'
    )
}}

-- select data
-----------------------------------------------
with calc_crypto_hodltime as (

    select * from {{ ref('calc_crypto_hodltime') }}

),

sat_crypto_kline as (

    select * from {{ ref('sat_crypto_kline') }}

),

-- apply calculation logic
-----------------------------------------------
base as (

    select
        cch.wallet,
        cch.coin,
        cch.sell_at,
        coalesce(
            case
                when cch.coin in ('USDT', 'BUSD', 'USDC')
                    then cch.sell_amount
                else cch.sell_amount * 0.5 * (scks.open_price + scks.close_price)
            end, 0
        ) as sell_price,
        cch.buy_at,
        coalesce(
            case
                when cch.coin in ('USDT', 'BUSD', 'USDC')
                    then cch.sell_amount
                else cch.sell_amount * 0.5 * (sckb.open_price + sckb.close_price)
            end, 0
        ) as buy_price,
        cch.selling_status,
        cch.hodltime_millisecond,
        cch.hodltime_readable
    from calc_crypto_hodltime as cch

    left join sat_crypto_kline as scks
        on (
            cch.coin = scks.coin
            and cch.sell_at between scks.kline_open_at and scks.kline_close_at
        )

    left join sat_crypto_kline as sckb
        on (
            cch.coin = sckb.coin
            and cch.buy_at between sckb.kline_open_at and sckb.kline_close_at
        )

),

final as (

    select
        wallet,
        coin,
        sell_at,
        sell_price,
        buy_at,
        buy_price,
        selling_status,
        sell_price - buy_price as profit,
        hodltime_millisecond,
        hodltime_readable,
        case
            when date_diff('year', buy_at, sell_at) < 1 --noqa: ST02
                then true
            else false
        end as is_taxable
    from base

)

select * from final
