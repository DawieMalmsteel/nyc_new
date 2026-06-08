-- Gold: dim_zone materialized as table.
{{ config(materialized='view') }}

select
  location_id,
  borough,
  zone,
  service_zone
from {{ ref('stg_zones') }}
