{{
    config(
        materialized = 'table',
        order_by = 'transaction_at'
    )
}}

-- select needed models
-----------------------------------------------
with sat_crypto_transaction as (

    select * from {{ ref('sat_crypto_transaction') }}

),

calc_crypto_transaction_accountbalance as (

    select * from {{ ref('calc_crypto_transaction_accountbalance') }}

),

-- join models to create final CTE
-----------------------------------------------
final as (

    select
        -- keys
        sct.fk_crypto_transaction,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts,
        -- measures
        sct.change_amount,
        cta.accountbalance,
        -- properties
        sct.coin,
        sct.transaction_at,
        sct.operation,
        sct.wallet
    from sat_crypto_transaction as sct

    inner join calc_crypto_transaction_accountbalance as cta
        on cta.fk_crypto_transaction = sct.fk_crypto_transaction
)

select * from final
