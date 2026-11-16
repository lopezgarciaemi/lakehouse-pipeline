{% snapshot snap_accounts %}

{{
    config(
      target_schema='snapshots',
      unique_key='account_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True,
    )
}}

select
    account_id,
    customer_name,
    account_tier,
    kyc_status,
    country_code,
    risk_segment,
    updated_at
from {{ ref('stg_accounts') }}

{% endsnapshot %}
