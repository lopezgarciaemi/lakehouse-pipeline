-- Singular data quality test: ensures every transaction has positive USD/MXN converted amounts.
select
    tx_id,
    amount_usd,
    amount_mxn
from {{ ref('fct_transactions') }}
where amount_usd is null
   or amount_mxn is null
   or amount_usd <= 0
   or amount_mxn <= 0
