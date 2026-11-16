select
    tx_date,
    currency,
    count(*) as transaction_count,
    sum(original_amount) as total_original_amount,
    round(sum(amount_usd), 2) as total_usd_equivalent,
    round(sum(amount_mxn), 2) as total_mxn_equivalent
from {{ ref('fct_transactions') }}
where status <> 'FAILED'
group by tx_date, currency
order by tx_date desc, total_usd_equivalent desc
