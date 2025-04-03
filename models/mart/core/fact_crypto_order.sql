{{
    config(
        materialized = 'table',
        order_by = 'order_at'
    )
}}

-- select needed models
-----------------------------------------------
with sat_crypto_order as (

    select * from {{ ref('sat_crypto_order') }}

),

-- join models to create final CTE
-----------------------------------------------
final as (

    select
        -- keys
        sco.fk_crypto_order,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts,
        -- measures
        sco.order_amount,
        sco.order_price,
        sco.executed_amount,
        sco.executed_price,
        sco.trade_amount,
        -- properties
        sco.order_coin,
        sco.trade_coin,
        sco.order_at,
        sco.platform,
        sco.wallet,
        sco.side,
        sco.pair,
        sco.type
    from sat_crypto_order as sco
)

select * from final
