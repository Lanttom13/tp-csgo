
-- Q1 — Top 20 équipes (winrate global, min 50 maps jouées)

SELECT
  t.team_name,
  ROUND(AVG(CASE WHEN f.is_winner THEN 1 ELSE 0 END)::numeric, 3) AS win_rate,
  COUNT(*) AS games
FROM dwh.fact_team_map_result f
JOIN dwh.dim_team t ON t.team_id = f.team_id
GROUP BY t.team_name
HAVING COUNT(*) >= 50
ORDER BY win_rate DESC
LIMIT 20;

-- résultat 

team_name	win_rate	games
aAa	0.000	78
Illuminar	0.000	140
Party Astronauts	0.000	60
x-kom	0.000	251
Singularity	0.000	454
Rugratz	0.000	79
Sharks	0.000	262
Seed	0.000	66
ORDER	0.000	308
Kings	0.000	66
Alpha Red	0.000	168
MVP Project	0.000	53
NASR	0.000	52
Epsilon	0.000	446
Binary Dragons	0.000	167
Furious	0.000	90
RED Canids	0.000	85
NiP	0.000	477
forZe	0.000	544
BOOT	0.000	109

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

-- résultat 


player_name	avg_rating	maps_played
ZywOo	1.379	394
s1mple	1.316	509
HuNtR	1.310	40
vsm	1.289	220
XANTARES	1.277	604
device	1.259	470
sh1ro	1.256	179
NiKo	1.246	489
huNter-	1.240	646
dexter	1.236	408
poizon	1.230	512
Sico	1.230	399
yuurih	1.226	410
DeathMakeR	1.225	33
degster	1.224	103
BnTeT	1.223	520
mantuu	1.223	181
Kaze	1.222	311
frozen	1.220	566
woxic	1.219	550


-- Q3 — “Week-end vs semaine” : winrate par map le week-end

SELECT
  mp.map_name,
  ROUND(AVG(CASE WHEN f.is_winner THEN 1 ELSE 0 END)::numeric, 3) AS win_rate_weekend,
  COUNT(*) AS games_weekend
FROM dwh.fact_team_map_result f
JOIN dwh.dim_date d ON d.date_sk = f.date_sk
JOIN dwh.dim_map mp ON mp.map_id = f.map_id
WHERE d.is_weekend = TRUE
GROUP BY mp.map_name
HAVING COUNT(*) >= 50
ORDER BY win_rate_weekend DESC;


-- résultat 


map_name	win_rate_weekend	games_weekend
Inferno	0.000	3478
Train	0.000	3042
Dust2	0.000	1838
Mirage	0.000	3958
Overpass	0.000	2514
Cache	0.000	1770
Nuke	0.000	2216
Vertigo	0.000	374
Cobblestone	0.000	1302
