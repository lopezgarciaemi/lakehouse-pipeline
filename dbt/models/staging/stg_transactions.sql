select
    tx_id,
    account_id,
    merchant_category,
    tx_type,
    upper(currency) as currency,
    cast(amount as double) as original_amount,
    status,
    cast(tx_timestamp as timestamptz) as tx_timestamp,
    cast(tx_timestamp as date) as tx_date
from read_parquet('{{ var("lakehouse_root") }}/bronze/transactions/dt=*/*.parquet', hive_partitioning = true)
qualify row_number() over (
    partition by tx_id
    order by tx_timestamp asc
) = 1
