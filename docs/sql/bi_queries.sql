
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



-- Q3 Top maps les plus jouées

SELECT
  m.map_name,
  COUNT(*) AS maps_played
FROM dwh.fact_match_map fmm
JOIN dwh.dim_map m ON m.map_id = fmm.map_id
GROUP BY m.map_name
ORDER BY maps_played DESC
LIMIT 15;

-- Q4 Évolution mensuelle du winrate (team x temps)

SELECT
  (d.year::text || '-' || LPAD(d.month::text, 2, '0')) AS year_month,
  t.team_name,
  ROUND(AVG(f.is_winner::int)::numeric, 3) AS win_rate,
  COUNT(*) AS games
FROM dwh.fact_team_map_result f
JOIN dwh.dim_date d ON d.date_sk = f.date_sk
JOIN dwh.dim_team t ON t.team_id = f.team_id
GROUP BY year_month, t.team_name
HAVING COUNT(*) >= 20
ORDER BY year_month ASC, t.team_name;



-- Q5 Heatmap winrate (team x map)

SELECT
  t.team_name,
  m.map_name,
  ROUND(AVG(f.is_winner::int)::numeric, 3) AS win_rate,
  COUNT(*) AS games
FROM dwh.fact_team_map_result f
JOIN dwh.dim_team t ON t.team_id = f.team_id
JOIN dwh.dim_map m ON m.map_id = f.map_id
GROUP BY t.team_name, m.map_name
HAVING COUNT(*) >= 20
ORDER BY t.team_name, m.map_name;



