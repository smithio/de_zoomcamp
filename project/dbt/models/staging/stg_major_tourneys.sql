{{ config(materialized='view') }}

SELECT
    {{ conv_date('tourney_date') }} AS tourney_date,
    tourney_name,
    tourney_level,
    winner_name
    
  FROM {{ source('staging', 'atp_matches') }}
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

 LIMIT 100

{% endif %}