-- top-100
CREATE TABLE `coral-muse-339314.tennis_prod.players_top` AS
WITH t_rank AS (
    SELECT ranking_date, rank, player, CAST(points AS INT64) AS points FROM `tennis_data.atp_rankings_70s`
    UNION ALL
    SELECT ranking_date, rank, player, CAST(points AS INT64) AS points FROM `tennis_data.atp_rankings_80s`
    UNION ALL
    SELECT ranking_date, rank, player, CAST(points AS INT64) AS points FROM `tennis_data.atp_rankings_90s`
    UNION ALL
    SELECT ranking_date, rank, player, CAST(points AS INT64) AS points FROM `tennis_data.atp_rankings_00s`
    UNION ALL
    SELECT ranking_date, rank, player, CAST(points AS INT64) AS points FROM `tennis_data.atp_rankings_10s`
    UNION ALL
    SELECT ranking_date, rank, player, CAST(points AS INT64) AS points FROM `tennis_data.atp_rankings_20s`
    UNION ALL
    SELECT ranking_date, rank, player, CAST(points AS INT64) AS points FROM `tennis_data.atp_rankings_current`
)
SELECT
    t_rank.ranking_date,
    t_rank.rank,
    CONCAT(t_plr.name_first, ' ', t_plr.name_last) AS player_name,
    t_rank.points,
    t_plr.ioc
  FROM t_rank
  JOIN `coral-muse-339314.tennis_data.atp_players` t_plr
    ON t_plr.player_id = t_rank.player
 WHERE t_rank.rank <= 100;

-- Grand slams
CREATE TABLE `coral-muse-339314.tennis_prod.major_tourneys` AS
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2022`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
 UNION ALL
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2021`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
 UNION ALL
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2020`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
 UNION ALL
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2019`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
 UNION ALL
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2018`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
 UNION ALL
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2017`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
 UNION ALL
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2016`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
 UNION ALL
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2015`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
 UNION ALL
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2014`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
 UNION ALL
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2013`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
 UNION ALL
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2012`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
 UNION ALL
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2011`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'
 UNION ALL
SELECT tourney_date, tourney_name, tourney_level, winner_name
  FROM `coral-muse-339314.tennis_data.atp_matches_2010`
 WHERE tourney_level IN ('G', 'M')
   AND round = 'F'

-- player_stats
CREATE TABLE `coral-muse-339314.tennis_prod.player_stats` AS
WITH t_matches AS (
  SELECT tourney_date, tourney_id, winner_name, winner_id, winner_rank, winner_rank_points, loser_name, loser_id, loser_rank, loser_rank_points, round, w_ace, w_df, w_svpt, w_1stin, l_ace, l_df, l_svpt, l_1stin, w_bpFaced, w_bpSaved, l_bpFaced, l_bpSaved FROM `coral-muse-339314.tennis_data.atp_matches_2022`
  UNION ALL
  SELECT tourney_date, tourney_id, winner_name, winner_id, winner_rank, winner_rank_points, loser_name, loser_id, loser_rank, loser_rank_points, round, w_ace, w_df, w_svpt, w_1stin, l_ace, l_df, l_svpt, l_1stin, w_bpFaced, w_bpSaved, l_bpFaced, l_bpSaved FROM `coral-muse-339314.tennis_data.atp_matches_2021`
  UNION ALL
  SELECT tourney_date, tourney_id, winner_name, winner_id, winner_rank, winner_rank_points, loser_name, loser_id, loser_rank, loser_rank_points, round, w_ace, w_df, w_svpt, w_1stin, l_ace, l_df, l_svpt, l_1stin, w_bpFaced, w_bpSaved, l_bpFaced, l_bpSaved FROM `coral-muse-339314.tennis_data.atp_matches_2020`
  UNION ALL
  SELECT tourney_date, tourney_id, winner_name, winner_id, winner_rank, winner_rank_points, loser_name, loser_id, loser_rank, loser_rank_points, round, w_ace, w_df, w_svpt, w_1stin, l_ace, l_df, l_svpt, l_1stin, w_bpFaced, w_bpSaved, l_bpFaced, l_bpSaved FROM `coral-muse-339314.tennis_data.atp_matches_2019`
  UNION ALL
  SELECT tourney_date, tourney_id, winner_name, winner_id, winner_rank, winner_rank_points, loser_name, loser_id, loser_rank, loser_rank_points, round, w_ace, w_df, w_svpt, w_1stin, l_ace, l_df, l_svpt, l_1stin, w_bpFaced, w_bpSaved, l_bpFaced, l_bpSaved FROM `coral-muse-339314.tennis_data.atp_matches_2018`
  UNION ALL
  SELECT tourney_date, tourney_id, winner_name, winner_id, winner_rank, winner_rank_points, loser_name, loser_id, loser_rank, loser_rank_points, round, w_ace, w_df, w_svpt, w_1stin, l_ace, l_df, l_svpt, l_1stin, w_bpFaced, w_bpSaved, l_bpFaced, l_bpSaved FROM `coral-muse-339314.tennis_data.atp_matches_2017`
  UNION ALL
  SELECT tourney_date, tourney_id, winner_name, winner_id, winner_rank, winner_rank_points, loser_name, loser_id, loser_rank, loser_rank_points, round, w_ace, w_df, w_svpt, w_1stin, l_ace, l_df, l_svpt, l_1stin, w_bpFaced, w_bpSaved, l_bpFaced, l_bpSaved FROM `coral-muse-339314.tennis_data.atp_matches_2016`
  UNION ALL
  SELECT tourney_date, tourney_id, winner_name, winner_id, winner_rank, winner_rank_points, loser_name, loser_id, loser_rank, loser_rank_points, round, w_ace, w_df, w_svpt, w_1stin, l_ace, l_df, l_svpt, l_1stin, w_bpFaced, w_bpSaved, l_bpFaced, l_bpSaved FROM `coral-muse-339314.tennis_data.atp_matches_2015`
),
t_match_stats AS (
SELECT
    tourney_date, tourney_id,
    winner_name AS player_name, winner_id AS player_id,
    1 AS cnt_match, 1 AS cnt_win,
    CASE WHEN round = 'F' THEN 1 ELSE 0 END AS cnt_titles,
    CASE WHEN round = 'F' THEN 1 ELSE 0 END AS cnt_finals,
    w_ace AS cnt_aces, w_df AS cnt_df,
    w_svpt AS cnt_svpt, w_1stin AS cnt_1stin,
    w_bpFaced AS cnt_bpFaced, w_bpSaved AS cnt_bpSaved,
    l_bpFaced AS cnt_bpHad, l_bpFaced-l_bpSaved AS cnt_bpConv,
    DENSE_RANK() OVER(ORDER BY tourney_date DESC) AS date_rank
  FROM t_matches
UNION ALL
SELECT
    tourney_date, tourney_id,
    loser_name AS player_name, loser_id AS player_id,
    1 AS cnt_match, 0 AS cnt_win,
    0 AS cnt_titles,
    CASE WHEN round = 'F' THEN 1 ELSE 0 END AS cnt_finals,
    l_ace AS cnt_aces, l_df AS cnt_df,
    l_svpt AS cnt_svpt, l_1stin AS cnt_1stin,
    l_bpFaced AS cnt_bpFaced, l_bpSaved AS cnt_bpSaved,
    w_bpFaced AS cnt_bpHad, w_bpFaced-w_bpSaved AS cnt_bpConv,
    DENSE_RANK() OVER(ORDER BY tourney_date DESC) AS date_rank
  FROM t_matches
),
t_tourn_stats AS (
  SELECT
    tourney_date, MIN(tourney_id) AS tourney_id, 1 AS tournaments_played,
    MIN(player_name) AS player_name, player_id,
    SUM(cnt_match) AS cnt_match, SUM(cnt_win) AS cnt_win,
    SUM(cnt_titles) AS cnt_titles, SUM(cnt_finals) AS cnt_finals,
    SUM(cnt_aces) AS cnt_aces, SUM(cnt_df) AS cnt_df,
    SUM(cnt_svpt) AS cnt_svpt, SUM(cnt_1stin) AS cnt_1stin,
    SUM(cnt_bpFaced) AS cnt_bpFaced, SUM(cnt_bpSaved) AS cnt_bpSaved,
    SUM(cnt_bpHad) AS cnt_bpHad, SUM(cnt_bpConv) AS cnt_bpConv,
    MIN(date_rank) AS date_rank
  FROM t_match_stats
 GROUP BY tourney_date, player_id
)
SELECT
    tourney_date, player_name, player_id,
    SUM(cnt_match) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS matches,
    SUM(cnt_win) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wins,
    ROUND(SUM(cnt_win) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / SUM(cnt_match) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS win_ratio,
    SUM(tournaments_played) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS tournaments,
    SUM(cnt_titles) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS titles,
    -- ~52 weeks == ideally need to convert ranking_date to date and do OVER(ORDER BY ranking_date RANGE INTERVAL 365 DAY AND CURRENT ROW)
    SUM(cnt_match) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) AS matches_52,
    SUM(cnt_win) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) AS wins_52,
    ROUND(SUM(cnt_win) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) / SUM(cnt_match) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW), 2) AS win_ratio_52,
    SUM(tournaments_played) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) AS tournaments_52,
    SUM(cnt_titles) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) AS titles_52,
    -- serve
    SUM(cnt_aces) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) AS aces,
    SUM(cnt_df) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) AS df,
    -- breakpoints
    SUM(cnt_bpFaced) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) AS bp_faced,
    SUM(cnt_bpSaved) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) AS bp_saved,
    ROUND(SUM(cnt_bpSaved) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) / NULLIF(SUM(cnt_bpFaced) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW), 0), 2) AS bp_ratio_saved,
    SUM(cnt_bpHad) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) AS bp_had,
    SUM(cnt_bpConv) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) AS bp_conv,
    ROUND(SUM(cnt_bpConv) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW) / NULLIF(SUM(cnt_bpHad) OVER(PARTITION BY player_name ORDER BY tourney_date ROWS BETWEEN 52 PRECEDING AND CURRENT ROW), 0), 2) AS bp_ratio_conv
  FROM t_tourn_stats

-- top_player_stats
CREATE TABLE `coral-muse-339314.tennis_prod.top_player_stats` AS
WITH t AS (
SELECT
    t1.ranking_date, t1.rank, t1.points, t1.ioc AS country,
    t2.*, RANK() OVER(PARTITION BY t1.ranking_date, t1.player_name ORDER BY t2.tourney_date DESC) AS rn
  FROM `coral-muse-339314.tennis_prod.players_top` t1
  LEFT JOIN `coral-muse-339314.tennis_prod.player_stats` t2
    ON t2.player_name = t1.player_name
   AND t2.tourney_date < t1.ranking_date
)
SELECT * FROM t WHERE rn = 1

-- current grand slams
CREATE TABLE `coral-muse-339314.tennis_prod.current_majors` AS
WITH t AS (
SELECT
    t1.ranking_date,
    t2.*, RANK() OVER(PARTITION BY t1.ranking_date, t2.tourney_name ORDER BY t2.tourney_date DESC) AS rn
  FROM (SELECT DISTINCT ranking_date FROM `coral-muse-339314.tennis_prod.players_top`) t1
  LEFT JOIN `coral-muse-339314.tennis_prod.major_tourneys` t2
    ON t2.tourney_date < t1.ranking_date
)
SELECT ranking_date, tourney_date, CAST(tourney_date/10000 AS INT64) AS year, tourney_name AS tournament, tourney_level, winner_name AS winner
  FROM t
 WHERE rn = 1