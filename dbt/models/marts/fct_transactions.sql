{{
    config(
        materialized='incremental',
        unique_key='tx_id',
        incremental_strategy='delete+insert'
    )
}}

select
    tx_id,
    account_id,
    customer_name,
    account_tier,
    risk_segment,
    merchant_category,
    tx_type,
    currency,
    original_amount,
    amount_usd,
    amount_mxn,
    status,
    tx_date,
    tx_timestamp
from {{ ref('int_transactions_enriched') }}

{% if is_incremental() %}
where tx_timestamp >= (select coalesce(max(tx_timestamp), timestamp '1970-01-01') - interval '1 day' from {{ this }})
{% endif %}
