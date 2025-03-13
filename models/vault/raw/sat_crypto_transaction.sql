{{
    config(
        materialized = 'table'
    )
}}

-- select data from sources
-----------------------------------------------
with binance_transactions_spot as (

    select * from {{ source('spark', 's3_binance_transactions_spot') }}

),

binance_transactions_futures as (

    select * from {{ source('spark', 's3_binance_transactions_futures') }}

),

-- implement logic to build the model
-----------------------------------------------
base_spot as (

    select
        *,
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
        cast(concat(b.unique_key, '_', cast(b.r as varchar)) as varchar(255)) as fk_crypto_transaction,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts,
        -- properties
        cast(b.transaction_utc_at as timestamp) at time zone 'Europe/Berlin' as transaction_at,
        cast('Spot' as varchar(255)) as wallet,
        cast(b.account as varchar(255)) as from_wallet,
        cast(b.operation as varchar(255)) as operation,
        cast(b.change as decimal(20, 8)) as change_amount,
        cast(b.coin as varchar(10)) as coin
    from base_spot as b

    union all

    select
        -- keys
        cast(concat(b.unique_key, '_', cast(b.r as varchar)) as varchar(255)) as fk_crypto_transaction,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts,
        -- properties
        cast(b.transaction_utc_at as timestamp) at time zone 'Europe/Berlin' as transaction_at,
        cast('USD-M Futures' as varchar(255)) as wallet,
        cast(case
            when type = 'TRANSFER'
                then 'Spot'
            else 'USD-M Futures'
        end as varchar(255)) as from_wallet,
        cast(b.type as varchar(255)) as operation,
        cast(b.amount as decimal(20, 8)) as change_amount,
        cast(b.asset as varchar(10)) as coin
    from base_futures as b

)

select * from final
