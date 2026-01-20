BEGIN;

CREATE SCHEMA IF NOT EXISTS dwh;

-- =================
-- Dimensions
-- =================
DROP TABLE IF EXISTS dwh.dim_team CASCADE;
CREATE TABLE dwh.dim_team (
  team_id SERIAL PRIMARY KEY,
  team_name TEXT UNIQUE NOT NULL
);

INSERT INTO dwh.dim_team(team_name)
SELECT DISTINCT trim(team_name)
FROM (
  SELECT team_1 AS team_name FROM staging.results
  UNION ALL SELECT team_2 FROM staging.results
  UNION ALL SELECT team_1 FROM staging.picks
  UNION ALL SELECT team_2 FROM staging.picks
  UNION ALL SELECT team_1 FROM staging.economy
  UNION ALL SELECT team_2 FROM staging.economy
  UNION ALL SELECT team FROM staging.players
  UNION ALL SELECT opponent FROM staging.players
) t
WHERE team_name IS NOT NULL AND trim(team_name) <> '';

DROP TABLE IF EXISTS dwh.dim_map CASCADE;
CREATE TABLE dwh.dim_map (
  map_id SERIAL PRIMARY KEY,
  map_name TEXT UNIQUE NOT NULL
);

INSERT INTO dwh.dim_map(map_name)
SELECT DISTINCT trim(map_name)
FROM (
  SELECT _map AS map_name FROM staging.results
  UNION ALL SELECT _map FROM staging.economy
  UNION ALL SELECT map_1 FROM staging.players
  UNION ALL SELECT map_2 FROM staging.players
  UNION ALL SELECT map_3 FROM staging.players
  UNION ALL SELECT t1_removed_1 FROM staging.picks
  UNION ALL SELECT t1_removed_2 FROM staging.picks
  UNION ALL SELECT t1_removed_3 FROM staging.picks
  UNION ALL SELECT t2_removed_1 FROM staging.picks
  UNION ALL SELECT t2_removed_2 FROM staging.picks
  UNION ALL SELECT t2_removed_3 FROM staging.picks
  UNION ALL SELECT t1_picked_1 FROM staging.picks
  UNION ALL SELECT t2_picked_1 FROM staging.picks
  UNION ALL SELECT left_over FROM staging.picks
) m
WHERE map_name IS NOT NULL AND trim(map_name) <> '';

DROP TABLE IF EXISTS dwh.dim_event CASCADE;
CREATE TABLE dwh.dim_event (
  event_id INT PRIMARY KEY,
  event_name TEXT
);

-- IMPORTANT: event_id vient de plusieurs CSV (picks/results/economy/players).
-- On prend l'union de tous les event_id, et on récupère le nom si dispo via players.
WITH all_events AS (
  SELECT DISTINCT NULLIF(regexp_replace(event_id, '[^0-9]', '', 'g'), '')::INT AS event_id
  FROM (
    SELECT event_id FROM staging.picks
    UNION ALL SELECT event_id FROM staging.results
    UNION ALL SELECT event_id FROM staging.economy
    UNION ALL SELECT event_id FROM staging.players
  ) e
  WHERE event_id IS NOT NULL AND trim(event_id) <> ''
),
names AS (
  SELECT
    NULLIF(regexp_replace(event_id, '[^0-9]', '', 'g'), '')::INT AS event_id,
    MAX(NULLIF(trim(event_name), ''))::TEXT AS event_name
  FROM staging.players
  WHERE event_id IS NOT NULL AND trim(event_id) <> ''
  GROUP BY NULLIF(regexp_replace(event_id, '[^0-9]', '', 'g'), '')::INT
)
INSERT INTO dwh.dim_event(event_id, event_name)
SELECT ae.event_id, n.event_name
FROM all_events ae
LEFT JOIN names n USING(event_id)
WHERE ae.event_id IS NOT NULL;

-- =================
-- Fact: match
-- =================
DROP TABLE IF EXISTS dwh.fact_match CASCADE;
CREATE TABLE dwh.fact_match (
  match_id INT PRIMARY KEY,
  match_date DATE,
  event_id INT REFERENCES dwh.dim_event(event_id),
  team1_id INT REFERENCES dwh.dim_team(team_id),
  team2_id INT REFERENCES dwh.dim_team(team_id),
  best_of INT,
  system TEXT,
  inverted_teams BOOLEAN,
  match_winner TEXT,
  map_wins_1 INT,
  map_wins_2 INT,
  rank_1 INT,
  rank_2 INT
);

WITH agg AS (
  SELECT
    NULLIF(regexp_replace(match_id, '[^0-9]', '', 'g'), '')::INT AS match_id,
    MAX(NULLIF(regexp_replace(map_wins_1, '[^0-9]', '', 'g'), '')::INT) AS map_wins_1,
    MAX(NULLIF(regexp_replace(map_wins_2, '[^0-9]', '', 'g'), '')::INT) AS map_wins_2,
    MAX(NULLIF(regexp_replace(rank_1, '[^0-9]', '', 'g'), '')::INT) AS rank_1,
    MAX(NULLIF(regexp_replace(rank_2, '[^0-9]', '', 'g'), '')::INT) AS rank_2,
    MAX(match_winner) AS match_winner
  FROM staging.results
  GROUP BY NULLIF(regexp_replace(match_id, '[^0-9]', '', 'g'), '')::INT
)
INSERT INTO dwh.fact_match
SELECT
  NULLIF(regexp_replace(p.match_id, '[^0-9]', '', 'g'), '')::INT AS match_id,
  COALESCE(
    to_date(NULLIF(p.date, ''), 'YYYY-MM-DD'),
    to_date(NULLIF(p.date, ''), 'DD/MM/YYYY')
  ) AS match_date,
  NULLIF(regexp_replace(p.event_id, '[^0-9]', '', 'g'), '')::INT AS event_id,
  t1.team_id,
  t2.team_id,
  NULLIF(regexp_replace(p.best_of, '[^0-9]', '', 'g'), '')::INT AS best_of,
  p.system,
  CASE
    WHEN lower(trim(p.inverted_teams)) IN ('true','t','1','yes','y') THEN TRUE
    WHEN lower(trim(p.inverted_teams)) IN ('false','f','0','no','n') THEN FALSE
    ELSE NULL
  END AS inverted_teams,
  a.match_winner,
  a.map_wins_1,
  a.map_wins_2,
  a.rank_1,
  a.rank_2
FROM staging.picks p
LEFT JOIN agg a
  ON a.match_id = NULLIF(regexp_replace(p.match_id, '[^0-9]', '', 'g'), '')::INT
JOIN dwh.dim_team t1 ON t1.team_name = trim(p.team_1)
JOIN dwh.dim_team t2 ON t2.team_name = trim(p.team_2)
WHERE NULLIF(regexp_replace(p.match_id, '[^0-9]', '', 'g'), '') IS NOT NULL;

-- =================
-- Fact: match_map (results.csv est map-level)
-- =================
DROP TABLE IF EXISTS dwh.fact_match_map CASCADE;
CREATE TABLE dwh.fact_match_map (
  match_id INT REFERENCES dwh.fact_match(match_id),
  map_id INT REFERENCES dwh.dim_map(map_id),
  result_1 INT,
  result_2 INT,
  map_winner TEXT,
  starting_ct TEXT,
  ct_1 INT,
  t_1 INT,
  ct_2 INT,
  t_2 INT,
  PRIMARY KEY (match_id, map_id)
);

INSERT INTO dwh.fact_match_map
SELECT
  r_mid.match_id,
  m.map_id,
  NULLIF(regexp_replace(r.result_1, '[^0-9]', '', 'g'), '')::INT,
  NULLIF(regexp_replace(r.result_2, '[^0-9]', '', 'g'), '')::INT,
  r.map_winner,
  r.starting_ct,
  NULLIF(regexp_replace(r.ct_1, '[^0-9]', '', 'g'), '')::INT,
  NULLIF(regexp_replace(r.t_1, '[^0-9]', '', 'g'), '')::INT,
  NULLIF(regexp_replace(r.ct_2, '[^0-9]', '', 'g'), '')::INT,
  NULLIF(regexp_replace(r.t_2, '[^0-9]', '', 'g'), '')::INT
FROM staging.results r
CROSS JOIN LATERAL (
  SELECT NULLIF(regexp_replace(r.match_id, '[^0-9]', '', 'g'), '')::INT AS match_id
) r_mid
JOIN dwh.fact_match fm ON fm.match_id = r_mid.match_id
JOIN dwh.dim_map m ON m.map_name = trim(r._map)
WHERE r_mid.match_id IS NOT NULL;

-- =================
-- Fact: veto_action (normaliser picks)
-- =================
DROP TABLE IF EXISTS dwh.fact_veto_action CASCADE;
CREATE TABLE dwh.fact_veto_action (
  match_id INT REFERENCES dwh.fact_match(match_id),
  action_idx INT,
  action_type TEXT,   -- ban|pick|decider
  team_id INT NULL REFERENCES dwh.dim_team(team_id),
  map_id INT REFERENCES dwh.dim_map(map_id),
  PRIMARY KEY (match_id, action_idx)
);

INSERT INTO dwh.fact_veto_action
SELECT fm.match_id, 1, 'ban',  t1.team_id, m.map_id
FROM staging.picks p
JOIN dwh.fact_match fm ON fm.match_id = NULLIF(regexp_replace(p.match_id, '[^0-9]', '', 'g'), '')::INT
JOIN dwh.dim_team t1 ON t1.team_name = trim(p.team_1)
JOIN dwh.dim_map m ON m.map_name = trim(p.t1_removed_1)
WHERE p.t1_removed_1 IS NOT NULL

UNION ALL
SELECT fm.match_id, 2, 'ban',  t2.team_id, m.map_id
FROM staging.picks p
JOIN dwh.fact_match fm ON fm.match_id = NULLIF(regexp_replace(p.match_id, '[^0-9]', '', 'g'), '')::INT
JOIN dwh.dim_team t2 ON t2.team_name = trim(p.team_2)
JOIN dwh.dim_map m ON m.map_name = trim(p.t2_removed_1)
WHERE p.t2_removed_1 IS NOT NULL

UNION ALL
SELECT fm.match_id, 3, 'ban',  t1.team_id, m.map_id
FROM staging.picks p
JOIN dwh.fact_match fm ON fm.match_id = NULLIF(regexp_replace(p.match_id, '[^0-9]', '', 'g'), '')::INT
JOIN dwh.dim_team t1 ON t1.team_name = trim(p.team_1)
JOIN dwh.dim_map m ON m.map_name = trim(p.t1_removed_2)
WHERE p.t1_removed_2 IS NOT NULL

UNION ALL
SELECT fm.match_id, 4, 'ban',  t2.team_id, m.map_id
FROM staging.picks p
JOIN dwh.fact_match fm ON fm.match_id = NULLIF(regexp_replace(p.match_id, '[^0-9]', '', 'g'), '')::INT
JOIN dwh.dim_team t2 ON t2.team_name = trim(p.team_2)
JOIN dwh.dim_map m ON m.map_name = trim(p.t2_removed_2)
WHERE p.t2_removed_2 IS NOT NULL

UNION ALL
SELECT fm.match_id, 5, 'ban',  t1.team_id, m.map_id
FROM staging.picks p
JOIN dwh.fact_match fm ON fm.match_id = NULLIF(regexp_replace(p.match_id, '[^0-9]', '', 'g'), '')::INT
JOIN dwh.dim_team t1 ON t1.team_name = trim(p.team_1)
JOIN dwh.dim_map m ON m.map_name = trim(p.t1_removed_3)
WHERE p.t1_removed_3 IS NOT NULL

UNION ALL
SELECT fm.match_id, 6, 'ban',  t2.team_id, m.map_id
FROM staging.picks p
JOIN dwh.fact_match fm ON fm.match_id = NULLIF(regexp_replace(p.match_id, '[^0-9]', '', 'g'), '')::INT
JOIN dwh.dim_team t2 ON t2.team_name = trim(p.team_2)
JOIN dwh.dim_map m ON m.map_name = trim(p.t2_removed_3)
WHERE p.t2_removed_3 IS NOT NULL

UNION ALL
SELECT fm.match_id, 7, 'pick', t1.team_id, m.map_id
FROM staging.picks p
JOIN dwh.fact_match fm ON fm.match_id = NULLIF(regexp_replace(p.match_id, '[^0-9]', '', 'g'), '')::INT
JOIN dwh.dim_team t1 ON t1.team_name = trim(p.team_1)
JOIN dwh.dim_map m ON m.map_name = trim(p.t1_picked_1)
WHERE p.t1_picked_1 IS NOT NULL

UNION ALL
SELECT fm.match_id, 8, 'pick', t2.team_id, m.map_id
FROM staging.picks p
JOIN dwh.fact_match fm ON fm.match_id = NULLIF(regexp_replace(p.match_id, '[^0-9]', '', 'g'), '')::INT
JOIN dwh.dim_team t2 ON t2.team_name = trim(p.team_2)
JOIN dwh.dim_map m ON m.map_name = trim(p.t2_picked_1)
WHERE p.t2_picked_1 IS NOT NULL

UNION ALL
SELECT fm.match_id, 9, 'decider', NULL, m.map_id
FROM staging.picks p
JOIN dwh.fact_match fm ON fm.match_id = NULLIF(regexp_replace(p.match_id, '[^0-9]', '', 'g'), '')::INT
JOIN dwh.dim_map m ON m.map_name = trim(p.left_over)
WHERE p.left_over IS NOT NULL;

-- =================
-- Fact: economy_round (unpivot 1..30)
-- =================
DROP TABLE IF EXISTS dwh.fact_economy_round CASCADE;
CREATE TABLE dwh.fact_economy_round (
  match_id INT REFERENCES dwh.fact_match(match_id),
  map_id INT REFERENCES dwh.dim_map(map_id),
  round_number INT,
  t1_value INT,
  t2_value INT,
  round_winner TEXT,
  t1_start TEXT,
  t2_start TEXT,
  PRIMARY KEY (match_id, map_id, round_number)
);

WITH base AS (
  SELECT
    NULLIF(regexp_replace(e.match_id, '[^0-9]', '', 'g'), '')::INT AS match_id,
    m.map_id,
    e.t1_start,
    e.t2_start,
    e."1_t1", e."1_t2", e."1_winner",
    e."2_t1", e."2_t2", e."2_winner",
    e."3_t1", e."3_t2", e."3_winner",
    e."4_t1", e."4_t2", e."4_winner",
    e."5_t1", e."5_t2", e."5_winner",
    e."6_t1", e."6_t2", e."6_winner",
    e."7_t1", e."7_t2", e."7_winner",
    e."8_t1", e."8_t2", e."8_winner",
    e."9_t1", e."9_t2", e."9_winner",
    e."10_t1", e."10_t2", e."10_winner",
    e."11_t1", e."11_t2", e."11_winner",
    e."12_t1", e."12_t2", e."12_winner",
    e."13_t1", e."13_t2", e."13_winner",
    e."14_t1", e."14_t2", e."14_winner",
    e."15_t1", e."15_t2", e."15_winner",
    e."16_t1", e."16_t2", e."16_winner",
    e."17_t1", e."17_t2", e."17_winner",
    e."18_t1", e."18_t2", e."18_winner",
    e."19_t1", e."19_t2", e."19_winner",
    e."20_t1", e."20_t2", e."20_winner",
    e."21_t1", e."21_t2", e."21_winner",
    e."22_t1", e."22_t2", e."22_winner",
    e."23_t1", e."23_t2", e."23_winner",
    e."24_t1", e."24_t2", e."24_winner",
    e."25_t1", e."25_t2", e."25_winner",
    e."26_t1", e."26_t2", e."26_winner",
    e."27_t1", e."27_t2", e."27_winner",
    e."28_t1", e."28_t2", e."28_winner",
    e."29_t1", e."29_t2", e."29_winner",
    e."30_t1", e."30_t2", e."30_winner"
  FROM staging.economy e
  JOIN dwh.dim_map m ON m.map_name = trim(e._map)
  JOIN dwh.fact_match fm ON fm.match_id = NULLIF(regexp_replace(e.match_id, '[^0-9]', '', 'g'), '')::INT
  WHERE NULLIF(regexp_replace(e.match_id, '[^0-9]', '', 'g'), '') IS NOT NULL
)
INSERT INTO dwh.fact_economy_round(match_id, map_id, round_number, t1_value, t2_value, round_winner, t1_start, t2_start)
SELECT match_id, map_id, r.round_number, r.t1_value, r.t2_value, r.round_winner, t1_start, t2_start
FROM base
CROSS JOIN LATERAL (
  VALUES
    ( 1, NULLIF(regexp_replace("1_t1",  '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("1_t2",  '[^0-9]', '', 'g'), '')::INT, "1_winner"),
    ( 2, NULLIF(regexp_replace("2_t1",  '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("2_t2",  '[^0-9]', '', 'g'), '')::INT, "2_winner"),
    ( 3, NULLIF(regexp_replace("3_t1",  '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("3_t2",  '[^0-9]', '', 'g'), '')::INT, "3_winner"),
    ( 4, NULLIF(regexp_replace("4_t1",  '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("4_t2",  '[^0-9]', '', 'g'), '')::INT, "4_winner"),
    ( 5, NULLIF(regexp_replace("5_t1",  '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("5_t2",  '[^0-9]', '', 'g'), '')::INT, "5_winner"),
    ( 6, NULLIF(regexp_replace("6_t1",  '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("6_t2",  '[^0-9]', '', 'g'), '')::INT, "6_winner"),
    ( 7, NULLIF(regexp_replace("7_t1",  '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("7_t2",  '[^0-9]', '', 'g'), '')::INT, "7_winner"),
    ( 8, NULLIF(regexp_replace("8_t1",  '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("8_t2",  '[^0-9]', '', 'g'), '')::INT, "8_winner"),
    ( 9, NULLIF(regexp_replace("9_t1",  '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("9_t2",  '[^0-9]', '', 'g'), '')::INT, "9_winner"),
    (10, NULLIF(regexp_replace("10_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("10_t2", '[^0-9]', '', 'g'), '')::INT, "10_winner"),
    (11, NULLIF(regexp_replace("11_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("11_t2", '[^0-9]', '', 'g'), '')::INT, "11_winner"),
    (12, NULLIF(regexp_replace("12_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("12_t2", '[^0-9]', '', 'g'), '')::INT, "12_winner"),
    (13, NULLIF(regexp_replace("13_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("13_t2", '[^0-9]', '', 'g'), '')::INT, "13_winner"),
    (14, NULLIF(regexp_replace("14_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("14_t2", '[^0-9]', '', 'g'), '')::INT, "14_winner"),
    (15, NULLIF(regexp_replace("15_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("15_t2", '[^0-9]', '', 'g'), '')::INT, "15_winner"),
    (16, NULLIF(regexp_replace("16_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("16_t2", '[^0-9]', '', 'g'), '')::INT, "16_winner"),
    (17, NULLIF(regexp_replace("17_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("17_t2", '[^0-9]', '', 'g'), '')::INT, "17_winner"),
    (18, NULLIF(regexp_replace("18_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("18_t2", '[^0-9]', '', 'g'), '')::INT, "18_winner"),
    (19, NULLIF(regexp_replace("19_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("19_t2", '[^0-9]', '', 'g'), '')::INT, "19_winner"),
    (20, NULLIF(regexp_replace("20_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("20_t2", '[^0-9]', '', 'g'), '')::INT, "20_winner"),
    (21, NULLIF(regexp_replace("21_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("21_t2", '[^0-9]', '', 'g'), '')::INT, "21_winner"),
    (22, NULLIF(regexp_replace("22_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("22_t2", '[^0-9]', '', 'g'), '')::INT, "22_winner"),
    (23, NULLIF(regexp_replace("23_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("23_t2", '[^0-9]', '', 'g'), '')::INT, "23_winner"),
    (24, NULLIF(regexp_replace("24_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("24_t2", '[^0-9]', '', 'g'), '')::INT, "24_winner"),
    (25, NULLIF(regexp_replace("25_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("25_t2", '[^0-9]', '', 'g'), '')::INT, "25_winner"),
    (26, NULLIF(regexp_replace("26_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("26_t2", '[^0-9]', '', 'g'), '')::INT, "26_winner"),
    (27, NULLIF(regexp_replace("27_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("27_t2", '[^0-9]', '', 'g'), '')::INT, "27_winner"),
    (28, NULLIF(regexp_replace("28_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("28_t2", '[^0-9]', '', 'g'), '')::INT, "28_winner"),
    (29, NULLIF(regexp_replace("29_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("29_t2", '[^0-9]', '', 'g'), '')::INT, "29_winner"),
    (30, NULLIF(regexp_replace("30_t1", '[^0-9]', '', 'g'), '')::INT, NULLIF(regexp_replace("30_t2", '[^0-9]', '', 'g'), '')::INT, "30_winner")
) AS r(round_number, t1_value, t2_value, round_winner)
WHERE r.t1_value IS NOT NULL OR r.t2_value IS NOT NULL;

-- =================
-- Fact: player_map (unpivot m1/m2/m3)
-- =================
DROP TABLE IF EXISTS dwh.fact_player_map CASCADE;
CREATE TABLE dwh.fact_player_map (
  match_id INT REFERENCES dwh.fact_match(match_id),
  map_id INT REFERENCES dwh.dim_map(map_id),
  player_id INT,
  player_name TEXT,
  country TEXT,
  team_id INT REFERENCES dwh.dim_team(team_id),
  opponent_name TEXT,
  kills INT,
  assists INT,
  deaths INT,
  kast NUMERIC,
  adr NUMERIC,
  rating NUMERIC,
  PRIMARY KEY (match_id, map_id, player_id)
);

WITH p AS (
  SELECT
    NULLIF(regexp_replace(player_id, '[^0-9]', '', 'g'), '')::INT AS player_id,
    player_name,
    country,
    NULLIF(regexp_replace(match_id, '[^0-9]', '', 'g'), '')::INT AS match_id,
    trim(team) AS team_name,
    trim(opponent) AS opponent_name,
    trim(map_1) AS map_name,
    NULLIF(regexp_replace(m1_kills,   '[^0-9]', '', 'g'), '')::INT AS kills,
    NULLIF(regexp_replace(m1_assists, '[^0-9]', '', 'g'), '')::INT AS assists,
    NULLIF(regexp_replace(m1_deaths,  '[^0-9]', '', 'g'), '')::INT AS deaths,
    NULLIF(regexp_replace(m1_kast,    '[^0-9.-]', '', 'g'), '')::NUMERIC AS kast,
    NULLIF(regexp_replace(m1_adr,     '[^0-9.-]', '', 'g'), '')::NUMERIC AS adr,
    NULLIF(regexp_replace(m1_rating,  '[^0-9.-]', '', 'g'), '')::NUMERIC AS rating
  FROM staging.players
  WHERE map_1 IS NOT NULL AND trim(map_1) <> ''

  UNION ALL
  SELECT
    NULLIF(regexp_replace(player_id, '[^0-9]', '', 'g'), '')::INT,
    player_name,
    country,
    NULLIF(regexp_replace(match_id, '[^0-9]', '', 'g'), '')::INT,
    trim(team),
    trim(opponent),
    trim(map_2),
    NULLIF(regexp_replace(m2_kills,   '[^0-9]', '', 'g'), '')::INT,
    NULLIF(regexp_replace(m2_assists, '[^0-9]', '', 'g'), '')::INT,
    NULLIF(regexp_replace(m2_deaths,  '[^0-9]', '', 'g'), '')::INT,
    NULLIF(regexp_replace(m2_kast,    '[^0-9.-]', '', 'g'), '')::NUMERIC,
    NULLIF(regexp_replace(m2_adr,     '[^0-9.-]', '', 'g'), '')::NUMERIC,
    NULLIF(regexp_replace(m2_rating,  '[^0-9.-]', '', 'g'), '')::NUMERIC
  FROM staging.players
  WHERE map_2 IS NOT NULL AND trim(map_2) <> ''

  UNION ALL
  SELECT
    NULLIF(regexp_replace(player_id, '[^0-9]', '', 'g'), '')::INT,
    player_name,
    country,
    NULLIF(regexp_replace(match_id, '[^0-9]', '', 'g'), '')::INT,
    trim(team),
    trim(opponent),
    trim(map_3),
    NULLIF(regexp_replace(m3_kills,   '[^0-9]', '', 'g'), '')::INT,
    NULLIF(regexp_replace(m3_assists, '[^0-9]', '', 'g'), '')::INT,
    NULLIF(regexp_replace(m3_deaths,  '[^0-9]', '', 'g'), '')::INT,
    NULLIF(regexp_replace(m3_kast,    '[^0-9.-]', '', 'g'), '')::NUMERIC,
    NULLIF(regexp_replace(m3_adr,     '[^0-9.-]', '', 'g'), '')::NUMERIC,
    NULLIF(regexp_replace(m3_rating,  '[^0-9.-]', '', 'g'), '')::NUMERIC
  FROM staging.players
  WHERE map_3 IS NOT NULL AND trim(map_3) <> ''
)
INSERT INTO dwh.fact_player_map
SELECT
  p.match_id,
  m.map_id,
  p.player_id,
  p.player_name,
  p.country,
  t.team_id,
  p.opponent_name,
  p.kills, p.assists, p.deaths, p.kast, p.adr, p.rating
FROM p
JOIN dwh.fact_match fm ON fm.match_id = p.match_id
JOIN dwh.dim_map m ON m.map_name = p.map_name
JOIN dwh.dim_team t ON t.team_name = p.team_name
WHERE p.match_id IS NOT NULL AND p.player_id IS NOT NULL;

COMMIT;
