{{
    config(
        materialized = 'table'
    )
}}

-- select data from sources
-----------------------------------------------
with binance_payins as (

    select * from {{ source('spark', 's3_binance_payins') }}

),

-- implement logic to build the model
-----------------------------------------------
final as (

    select
        -- keys
        cast(concat('binance_', order_id) as varchar(255)) as pk_crypto_payin,
        cast(concat('binance_', order_id) as varchar(255)) as bk_crypto_payin,
        -- metadata
        '{{ invocation_id }}' as record_source,
        current_timestamp at time zone 'Europe/Berlin' as load_dts
    from binance_payins
    where status = 'Successful'

)

select * from final
