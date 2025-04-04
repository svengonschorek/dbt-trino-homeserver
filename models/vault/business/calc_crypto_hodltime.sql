{{
    config(
        materialized = 'table'
    )
}}

-- select data
-----------------------------------------------
with calc_crypto_accountbalance as (

    select * from {{ ref('calc_crypto_accountbalance') }}

),

-- apply calculation logic
-----------------------------------------------
final as (

    select
        *
    from calc_crypto_accountbalance

)

select * from fifo
