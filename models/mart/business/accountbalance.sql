-- accountbalance model
{{
    config(
        materialized = 'table'
    )
}}

-- select needed models
-----------------------------------------------
with accountbalance_per_coin as (

    select * from {{ ref('accountbalance_per_coin') }}

),

-- build final CTE
-----------------------------------------------
final as (

    select
        kline_open_at,
        kline_close_at,
        sum(usdt_balance_open) as usdt_balance_open,
        sum(usdt_balance_close) as usdt_balance_close,
        sum(usdt_payin_or_payout) as usdt_payin_or_payout
    from accountbalance_per_coin
    group by
        kline_open_at,
        kline_close_at

)

select * from final
