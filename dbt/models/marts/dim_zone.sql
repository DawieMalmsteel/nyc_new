-- Mart: zone dimension with SCD Type 2 structure.
-- ponytail: view-only (Hive can't RENAME TABLE). Full SCD requires Postgres/Iceberg backend.
-- Columns valid_from/valid_to/is_current ready — switch to materialized=table when on Postgres.
{{ config(materialized='view') }}

select
    location_id as zone_id,
    borough,
    zone,
    service_zone,
    cast('2024-01-01' as date) as valid_from,
    cast(null as date) as valid_to,
    true as is_current
from {{ ref('stg_zones') }}
