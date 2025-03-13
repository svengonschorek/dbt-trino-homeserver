{{
    config(
        materialized = 'table'
    )
}}

-- select data from sources
-----------------------------------------------
with binance_transactions_spot as (

    select * from {{ source('airbyte', 's3_binance_transactions_spot') }}

),

binance_transactions_futures as (

    select * from {{ source('airbyte', 's3_binance_transactions_futures') }}

),

-- implement logic to build the model
-----------------------------------------------
base_spot as (

    select
        concat(
            'binance',
            '_',
            lower(replace(account, ' ', '_')),
            '_',
            lower(coin),
            '_',
            lower(replace(operation, ' ', '_')),
            '_',
            cast(to_unixtime(cast(transaction_utc_at as timestamp)) as varchar)
        ) as unique_key,
        row_number() over (
            partition by
                account,
                coin,
                operation,
                transaction_utc_at
            order by
                change
        ) as r
    from binance_transactions_spot

),

base_futures as (

    select
        *,
        transaction_id as unique_key,
        row_number() over (
            partition by
                transaction_id
            order by
                amount,
                type
        ) as r
    from binance_transactions_futures
),

final as (

    select
        -- keys
        cast(concat(unique_key, '_', cast(r as varchar)) as varchar(255)) as pk_crypto_transaction,
        cast(concat(unique_key, '_', cast(r as varchar)) as varchar(255)) as bk_crypto_transaction,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts
    from base_spot

    union all

    select
        -- keys
        cast(concat(unique_key, '_', cast(r as varchar)) as varchar(255)) as pk_crypto_transaction,
        cast(concat(unique_key, '_', cast(r as varchar)) as varchar(255)) as bk_crypto_transaction,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts
    from base_futures

)

select * from final
