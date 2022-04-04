{{ config(materialized='table') }}

WITH t AS (
    SELECT
        t1.ranking_date,
        t2.*, RANK() OVER(PARTITION BY t1.ranking_date, t2.tourney_name ORDER BY t2.tourney_date DESC) AS rn
    FROM {{ ref('stg_ranking_dates') }} t1
    LEFT JOIN {{ ref('stg_major_tourneys') }} t2
        ON t2.tourney_date < t1.ranking_date
)
SELECT
    ranking_date,
    tourney_date,
    EXTRACT(YEAR FROM tourney_date) AS year,
    tourney_name AS tournament,
    tourney_level,
    winner_name AS winner
  FROM t
 WHERE rn = 1