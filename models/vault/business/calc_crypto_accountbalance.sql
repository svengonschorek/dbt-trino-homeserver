{{
    config(
        materialized = 'table'
    )
}}

-- select data
-----------------------------------------------
with sat_crypto_transaction as (

    select * from {{ ref('sat_crypto_transaction') }}

),

-- apply calculation logic
-----------------------------------------------
base as (

    select
        -- properties
        transaction_at,
        wallet,
        coin,
        sum(change_amount) as change_amount
    from sat_crypto_transaction
    group by
        transaction_at,
        wallet,
        coin

),

final as (

    select
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts,
        -- properties
        transaction_at,
        wallet,
        coin,
        change_amount,
        sum(change_amount) over (
            partition by
                wallet,
                coin
            order by
                transaction_at asc,
                change_amount desc
        ) as accountbalance,
        transaction_at as valid_from,
        lead(transaction_at) over (
            partition by
                wallet,
                coin
            order by
                transaction_at
        ) as valid_to
    from base

)

select * from final
