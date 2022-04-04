{{ config(materialized='table') }}

WITH t_match_stats AS (
SELECT
    {{ conv_date('tourney_date') }} AS tourney_date, tourney_id,
    winner_name AS player_name, winner_id AS player_id,
    1 AS cnt_match, 1 AS cnt_win,
    CASE WHEN round = 'F' THEN 1 ELSE 0 END AS cnt_titles,
    CASE WHEN round = 'F' THEN 1 ELSE 0 END AS cnt_finals,
    w_ace AS cnt_aces, w_df AS cnt_df,
    w_bpFaced AS cnt_bpFaced, w_bpSaved AS cnt_bpSaved,
    l_bpFaced AS cnt_bpHad, l_bpFaced-l_bpSaved AS cnt_bpConv
  FROM {{ source('staging', 'atp_matches') }}
UNION ALL
SELECT
    {{ conv_date('tourney_date') }} AS tourney_date, tourney_id,
    loser_name AS player_name, loser_id AS player_id,
    1 AS cnt_match, 0 AS cnt_win,
    0 AS cnt_titles,
    CASE WHEN round = 'F' THEN 1 ELSE 0 END AS cnt_finals,
    l_ace AS cnt_aces, l_df AS cnt_df,
    l_bpFaced AS cnt_bpFaced, l_bpSaved AS cnt_bpSaved,
    w_bpFaced AS cnt_bpHad, w_bpFaced-w_bpSaved AS cnt_bpConv
  FROM {{ source('staging', 'atp_matches') }}
),
t_tourn_stats AS (
  SELECT
    tourney_date, MIN(tourney_id) AS tourney_id, 1 AS tournaments,
    MIN(player_name) AS player_name, player_id,
    SUM(cnt_match) AS cnt_match, SUM(cnt_win) AS cnt_win,
    SUM(cnt_titles) AS cnt_titles, SUM(cnt_finals) AS cnt_finals,
    SUM(cnt_aces) AS cnt_aces, SUM(cnt_df) AS cnt_df,
    SUM(cnt_bpFaced) AS cnt_bpFaced, SUM(cnt_bpSaved) AS cnt_bpSaved,
    SUM(cnt_bpHad) AS cnt_bpHad, SUM(cnt_bpConv) AS cnt_bpConv
  FROM t_match_stats
 GROUP BY tourney_date, player_id
)
SELECT
    -- meta
    t1.ranking_date,
    t1.player_id,
    MIN(t1.rank) AS rank,
    MIN(t1.player_name) AS player_name,
    MIN(t1.points) AS points,
    MIN(t1.country) AS country,
    -- career match stats
    SUM(t2.cnt_match) AS matches,
    SUM(t2.cnt_win) AS wins,
    ROUND(SUM(t2.cnt_win) / NULLIF(SUM(t2.cnt_match), 0), 2) AS win_ratio,
    SUM(t2.tournaments) AS tournaments,
    SUM(t2.cnt_titles) AS titles,
    -- last 52 weeks match stats
    {{ sum_52('cnt_match') }} AS matches_52,
    {{ sum_52('cnt_win') }} AS wins_52,
    ROUND({{ sum_52('cnt_win') }} / NULLIF({{ sum_52('cnt_match') }}, 0), 2) AS win_ratio_52,
    {{ sum_52('tournaments') }} AS tournaments_52,
    {{ sum_52('cnt_titles') }} AS titles_52,
    -- serve - last 52 weeks
    {{ sum_52('cnt_aces') }} AS aces,
    {{ sum_52('cnt_df') }} AS df,
    -- breakpoints - last 52 weeks
    {{ sum_52('cnt_bpFaced') }} AS bp_faced,
    {{ sum_52('cnt_bpSaved') }} AS bp_saved,
    ROUND({{ sum_52('cnt_bpSaved') }} / NULLIF({{ sum_52('cnt_bpFaced') }}, 0), 2) AS bp_ratio_saved,
    {{ sum_52('cnt_bpHad') }} AS bp_had,
    {{ sum_52('cnt_bpConv') }} AS bp_conv,
    ROUND({{ sum_52('cnt_bpConv') }} / NULLIF({{ sum_52('cnt_bpHad') }}, 0), 2) AS bp_ratio_conv
    
  FROM {{ ref('stg_top_players') }} t1
  LEFT JOIN t_tourn_stats t2
    ON t2.player_id = t1.player_id
   AND t2.tourney_date + INTERVAL 5 DAY < t1.ranking_date
 GROUP BY t1.ranking_date, t1.player_id