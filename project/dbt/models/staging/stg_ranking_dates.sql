{{ config(materialized='view') }}

SELECT
    DISTINCT {{ conv_date('ranking_date') }} AS ranking_date
  FROM {{ source('staging', 'atp_rankings') }}
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

 LIMIT 100

{% endif %}