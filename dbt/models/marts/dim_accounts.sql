select
    account_id,
    customer_name,
    account_tier,
    kyc_status,
    country_code,
    risk_segment,
    updated_at
from {{ ref('stg_accounts') }}
