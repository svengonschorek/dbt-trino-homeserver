{{
    config(
        materialized = 'table'
    )
}}

-- select data from sources
-----------------------------------------------
with binance_payins as (

    select * from {{ source('airbyte', 's3_binance_payins') }}

),

-- implement logic to build the model
-----------------------------------------------
final as (

    select
        -- keys
        concat('binance_', order_id) as fk_crypto_payin,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts,
        -- properties
        'binance' as platform,
        cast(payin_local_at as timestamp) as payin_at,
        cast(amount as decimal(10, 2)) as amount,
        cast(fee as decimal(10, 2)) as fee_amount,
        cast(coin as varchar(10)) as coin,
        cast(payment_method as varchar(255)) as payment_method
    from binance_payins
    where status = 'Successful'

)

select * from final
