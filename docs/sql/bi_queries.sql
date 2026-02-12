
-- Q1 (winrate global)

SELECT
  t.team_name,
  ROUND(AVG(f.is_winner::int)::numeric, 3) AS win_rate,
  COUNT(*) AS games
FROM dwh.fact_team_map_result f
JOIN dwh.dim_team t ON t.team_id = f.team_id
GROUP BY t.team_name
HAVING COUNT(*) >= 50
ORDER BY win_rate DESC
LIMIT 20;


-- Q2 — Top joueurs (rating moyen) avec volume minimum

SELECT
  p.player_name,
  ROUND(AVG(f.rating)::numeric, 3) AS avg_rating,
  COUNT(*) AS maps_played
FROM dwh.fact_player_map_star f
JOIN dwh.dim_player p ON p.player_id = f.player_id
WHERE f.rating IS NOT NULL
GROUP BY p.player_name
HAVING COUNT(*) >= 30
ORDER BY avg_rating DESC
LIMIT 20;



-- Q3 “winrate par map le week-end”

SELECT
  mp.map_name,
  ROUND(AVG(ABS(f.round_diff))::numeric, 2) AS avg_round_gap_weekend,
  COUNT(*) AS team_rows_weekend
FROM dwh.fact_team_map_result f
JOIN dwh.dim_date d ON d.date_sk = f.date_sk
JOIN dwh.dim_map mp ON mp.map_id = f.map_id
WHERE d.is_weekend = TRUE
GROUP BY mp.map_name
HAVING COUNT(*) >= 100
ORDER BY avg_round_gap_weekend DESC;


