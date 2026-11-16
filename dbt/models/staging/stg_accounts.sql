select
    account_id,
    customer_name,
    account_tier,
    kyc_status,
    country_code,
    risk_segment,
    cast(updated_at as timestamptz) as updated_at
from read_parquet('{{ var("lakehouse_root") }}/bronze/accounts/dt=*/*.parquet', hive_partitioning = true)
qualify row_number() over (
    partition by account_id
    order by updated_at desc, dt desc
) = 1
