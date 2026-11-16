select
    ingestion_date as rate_date,
    base_currency,
    target_currency,
    rate_per_usd,
    source
from {{ ref('stg_fx_rates') }}
