with fx_with_mxn as (
    select
        ingestion_date,
        target_currency,
        rate_per_usd,
        max(case when target_currency = 'MXN' then rate_per_usd end)
            over (partition by ingestion_date) as usd_to_mxn_rate
    from {{ ref('stg_fx_rates') }}
)

select
    t.tx_id,
    t.account_id,
    a.customer_name,
    a.account_tier,
    a.kyc_status,
    a.country_code,
    a.risk_segment,
    t.merchant_category,
    t.tx_type,
    t.currency,
    t.original_amount,
    fx.rate_per_usd,
    round(t.original_amount / fx.rate_per_usd, 4) as amount_usd,
    round((t.original_amount / fx.rate_per_usd) * fx.usd_to_mxn_rate, 2) as amount_mxn,
    t.status,
    t.tx_date,
    t.tx_timestamp
from {{ ref('stg_transactions') }} t
left join {{ ref('stg_accounts') }} a
    on t.account_id = a.account_id
left join fx_with_mxn fx
    on t.tx_date = fx.ingestion_date
    and t.currency = fx.target_currency
