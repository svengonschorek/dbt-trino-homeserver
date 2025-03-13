{{
    config(
        materialized = 'table',
        order_by = 'fk_crypto_transaction'
    )
}}

-- select data
-----------------------------------------------
with sat_crypto_transaction as (

    select * from {{ ref('sat_crypto_transaction') }}

),

-- apply calculation logic
-----------------------------------------------

final as (

    select
        -- keys
        fk_crypto_transaction,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts,
        -- properties
        sum(change_amount) over (
            partition by
                wallet,
                coin
            order by
                transaction_at asc,
                change_amount desc
        ) as accountbalance
    from sat_crypto_transaction

)

select * from final
