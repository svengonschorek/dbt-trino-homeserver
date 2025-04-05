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
buy_transactions as (

    select
        *,
        sum(change_amount) over (
            partition by
                wallet,
                coin
            order by
                transaction_at
            rows between unbounded preceding and current row
        ) as rolling_buy_amount
    from calc_crypto_accountbalance
    where change_amount > 0

),

sell_transactions_base as (

    select
        coin,
        wallet,
        transaction_at,
        change_amount,
        'realized' as selling_status
    from calc_crypto_accountbalance
    where change_amount < 0

    union all

    select
        coin,
        wallet,
        cast(date_trunc('day', current_date) as timestamp) as transaction_at,
        -1 * accountbalance as change_amount,
        'unrealized' as selling_status
    from calc_crypto_accountbalance
    where valid_to is null

),

sell_transactions as (

    select
        coin,
        wallet,
        transaction_at,
        change_amount,
        sum(change_amount) over (
            partition by
                wallet,
                coin
            order by
                transaction_at
            rows between unbounded preceding and current row
        ) as rolling_sell_amount,
        selling_status
    from sell_transactions_base

),

fifo as (

    select
        st.transaction_at as sell_at,
        bt.transaction_at as buy_at,
        st.wallet,
        st.coin,
        st.change_amount as total_sell_amount,
        st.selling_status,
        bt.change_amount as buy_amount,
        st.rolling_sell_amount,
        bt.rolling_buy_amount + st.rolling_sell_amount - st.change_amount as rolling_remaining_amount,
        lag(bt.rolling_buy_amount + st.rolling_sell_amount - st.change_amount) over (
            partition by
                st.wallet,
                st.coin,
                st.transaction_at
            order by
                bt.transaction_at
        ) as prev_rolling_remaining_amount,
        st.change_amount + bt.rolling_buy_amount + st.rolling_sell_amount - st.change_amount as left_to_sell
    from sell_transactions as st

    inner join buy_transactions as bt
        on (
            bt.wallet = st.wallet
            and bt.coin = st.coin
            and bt.transaction_at <= st.transaction_at
        )
    -- filter previously already sold coins
    where
        abs(st.rolling_sell_amount - st.change_amount) < bt.rolling_buy_amount

),

final as (

    select
        wallet,
        coin,
        sell_at,
        buy_at,
        case
            when buy_amount >= rolling_remaining_amount
                then rolling_remaining_amount
            when buy_amount <= rolling_remaining_amount and left_to_sell <= 0
                then buy_amount
            when buy_amount < rolling_remaining_amount and left_to_sell > 0
                then buy_amount - left_to_sell
        end as sell_amount,
        date_diff('millisecond', buy_at, sell_at) as hodltime_millisecond,
        human_readable_seconds(date_diff('second', buy_at, sell_at)) as hodltime_readable,
        selling_status
    from fifo
    where
        coalesce(prev_rolling_remaining_amount, 0) <= abs(total_sell_amount)

)

select * from final
