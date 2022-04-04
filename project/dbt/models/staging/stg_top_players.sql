{{ config(materialized='view') }}

SELECT
    {{ conv_date('t_rank.ranking_date') }} AS ranking_date,
    t_rank.rank,
    t_plr.player_id,
    CONCAT(t_plr.name_first, ' ', t_plr.name_last) AS player_name,
    t_rank.points,
    t_plr.ioc AS country
    
  FROM {{ source('staging', 'atp_rankings') }} t_rank
  JOIN {{ source('staging', 'atp_players') }} t_plr
    ON t_plr.player_id = t_rank.player
 WHERE t_rank.rank <= 100
-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

 LIMIT 100

{% endif %}