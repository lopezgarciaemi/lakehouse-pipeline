select
    cast(rate_date as date) as rate_date,
    cast(ingestion_date as date) as ingestion_date,
    upper(base_currency) as base_currency,
    upper(target_currency) as target_currency,
    cast(rate as double) as rate_per_usd,
    source
from read_parquet('{{ var("lakehouse_root") }}/bronze/fx_rates/dt=*/*.parquet', hive_partitioning = true)
qualify row_number() over (
    partition by ingestion_date, target_currency
    order by rate_date desc
) = 1
