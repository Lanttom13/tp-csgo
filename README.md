# TP-CSGO — Data Mark (ETL → REL → DWH → BI)

Ce dépôt met en place une chaîne complète de traitement de données CSGO :
- **Staging** : chargement des CSV bruts
- **Modèle relationnel (REL)** : normalisation des données
- **Data Warehouse (DWH)** : modèle dimensionnel (schéma étoile)
- **BI** : KPI & dashboard sous **Metabase**
- **Observabilité** : centralisation et lecture des logs via **Dozzle**

> Ateliers couverts : **Atelier 1 / 2 / 3**  
> Atelier 2 = DWH (étoile) + requêtes BI  
> Atelier 3 = **Metabase (KPI + Dashboard)** + **Dozzle (preuve d’erreur DB)**

---

## Sommaire

1. [Objectifs](#1-objectifs)  
2. [Prérequis](#2-prérequis)  
3. [Architecture Docker](#3-architecture-docker)  
4. [Structure du projet](#4-structure-du-projet)  
5. [Mise en place (pas à pas)](#5-mise-en-place-pas-à-pas)  
6. [ETL — Chargement CSV → Staging](#6-etl--chargement-csv--staging)  
7. [Transformation — Staging → REL](#7-transformation--staging--rel)  
8. [Transformation — REL → DWH (schéma étoile)](#8-transformation--rel--dwh-schéma-étoile)  
9. [Atelier 2 — Modèle étoile & cohérence des faits](#9-atelier-2--modèle-étoile--cohérence-des-faits)  
10. [Atelier 3 — BI (Metabase) & Logs (Dozzle)](#10-atelier-3--bi-metabase--logs-dozzle)  
11. [Livrables attendus (captures / SQL)](#11-livrables-attendus-captures--sql)  
12. [Reset / Nettoyage](#12-reset--nettoyage)

---

## 1. Objectifs

- Charger des données CSGO (CSV) dans Postgres
- Construire un **modèle relationnel** exploitable (schéma `rel`)
- Construire un **DWH en schéma étoile** (schéma `dwh`)
- Exposer des **KPI BI** via Metabase (Atelier 3)
- Fournir une **preuve log** via Dozzle (Atelier 3)

---

## 2. Prérequis

- Docker + Docker Compose
- (Optionnel) WSL2 sous Windows
- Un navigateur web (Metabase / Adminer / Dozzle)

---

## 3. Architecture Docker

Services :

- **postgres** : base de données (port `5432`)
- **adminer** : client SQL web (port `8080`)
- **metabase** : BI / dashboards (port `3000`)
- **dozzle** : viewer de logs Docker (port `9999`)
- **etl** : conteneur Python (chargement staging)

Accès (depuis la machine hôte) :

- Adminer : `http://localhost:8080`
- Metabase : `http://localhost:3000`
- Dozzle : `http://localhost:9999`

---

## 4. Structure du projet


TP-CSGO/
├─ data/
│ └─ raw/ # CSV bruts (results/picks/economy/players)
├─ docs/
│ ├─ diagrams/ # captures & schémas (preuves)
│ └─ sql/ # requêtes BI (Q1→Q5)
├─ scripts/
│ ├─ load_csv_to_postgres.py # CSV → staging.*
│ ├─ transform_to_rel.sql # staging → rel
│ └─ transform_to_dwh.sql # rel → dwh (étoile + tables de faits)
├─ docker-compose.yml
├─ .env.example
├─ README.md
└─ requirements.txt
---

## 5. Mise en place et exécution

### 5.1 Prérequis
- Docker Desktop installé
- (WSL2) Intégration WSL activée dans Docker Desktop :
  `Settings → Resources → WSL integration → Ubuntu ON`

### 5.2 Configuration (.env)
Créer un fichier `.env` (non versionné) à la racine du projet (ou copier `.env.example`) :

```env
POSTGRES_DB=csgo
POSTGRES_USER=csgo
POSTGRES_PASSWORD=change_me
POSTGRES_PORT=5432

### 5.3 Démarrage des services

```bash
docker compose up -d
docker compose ps
```

### 5.4 Étape 2 – Import CSV vers staging

Placer les CSV dans data/raw/

Lancer l’import (ETL Python) :

```bash
docker compose run --rm etl
```

Résultat : création/remplissage des tables brutes :

staging.results

staging.picks

staging.economy

staging.players

### 5.5 Étape 3 – Transformation vers rel

Script : `scripts/transform_to_rel.sql`

```bash
docker compose run --rm etl bash -lc "apt-get update >/dev/null 2>&1 && apt-get install -y postgresql-client >/dev/null 2>&1 && PGPASSWORD=\$POSTGRES_PASSWORD psql -h postgres -U \$POSTGRES_USER -d \$POSTGRES_DB -f scripts/transform_to_rel.sql"
```

Résultat : création/remplissage du schéma rel avec PK/FK et tables d’association.


## 6. Modèle relationnel (schéma rel)

![MCD - schéma relationnel (rel)](docs/diagrams/MCD.PNG)

### 6.1 Entités

rel.team(team_id, team_name)

rel.map(map_id, map_name)

rel.event(event_id, event_name)

rel.match(match_id, match_date, event_id, best_of, system, inverted_teams)

rel.player(player_id, player_name, country)

### 6.2 Tables d’association

rel.match_team(match_id, team_slot, team_id, rank, map_wins, is_winner)
→ association Match ↔ Team (2 lignes par match)

rel.match_map(match_id, map_id, team1_score, team2_score, starting_ct_slot, winner_slot, ...)
→ association Match ↔ Map (résultats map-level)

rel.veto_action(match_id, action_idx, action_type, team_id, map_id)
→ normalisation de la séquence ban/pick/decider

rel.economy_round(match_id, map_id, round_number, team1_value, team2_value, winner_slot, ...)
→ économie par round (unpivot 1..30)

rel.player_map(match_id, map_id, player_id, team_id, opponent_team_id, kills, assists, deaths, kast, adr, rating)
→ stats joueur par map (unpivot m1/m2/m3)


### 6.3 ERD (Mermaid)

```mermaid
erDiagram
  EVENT ||--o{ MATCH : hosts

  MATCH ||--o{ MATCH_TEAM : has
  TEAM  ||--o{ MATCH_TEAM : participates

  MATCH ||--o{ MATCH_MAP : includes
  MAP   ||--o{ MATCH_MAP : played_on

  MATCH ||--o{ VETO_ACTION : has
  TEAM  ||--o{ VETO_ACTION : performs
  MAP   ||--o{ VETO_ACTION : concerns

  MATCH_MAP ||--o{ ECONOMY_ROUND : has

  PLAYER ||--o{ PLAYER_MAP : has
  MATCH  ||--o{ PLAYER_MAP : includes
  MAP    ||--o{ PLAYER_MAP : on
  TEAM   ||--o{ PLAYER_MAP : for


```
## 7. Validations et preuves

### 7.1 Comptage des tables

```bash
docker compose exec postgres psql -U csgo -d csgo -c "
SELECT 'team' t, COUNT(*) FROM rel.team
UNION ALL SELECT 'map', COUNT(*) FROM rel.map
UNION ALL SELECT 'event', COUNT(*) FROM rel.event
UNION ALL SELECT 'player', COUNT(*) FROM rel.player
UNION ALL SELECT 'match', COUNT(*) FROM rel.match
UNION ALL SELECT 'match_team', COUNT(*) FROM rel.match_team
UNION ALL SELECT 'match_map', COUNT(*) FROM rel.match_map
UNION ALL SELECT 'veto_action', COUNT(*) FROM rel.veto_action
UNION ALL SELECT 'economy_round', COUNT(*) FROM rel.economy_round
UNION ALL SELECT 'player_map', COUNT(*) FROM rel.player_map;"
```

Résultats observés :

team = 4173

map = 37

event = 3185

player = 12295

match = 16035

match_team = 32070

match_map = 30355

veto_action = 144315

economy_round = 626381

player_map = 295023

### 7.2 Preuve relationnelle : match ↔ team

```bash
Dans un modèle relationnel, le lien match/équipes est représenté par une table d’association.


docker compose exec postgres psql -U csgo -d csgo -c "
SELECT match_id, COUNT(*) AS nb_teams
FROM rel.match_team
GROUP BY match_id
ORDER BY nb_teams DESC
LIMIT 5;"
```

Résultat observé : nb_teams = 2 (conforme).


### 7.3 Limitation du dataset : economy

Le fichier economy.csv ne couvre pas l’intégralité des matchs du dataset : certaines rencontres n’ont pas d’informations d’économie.
Le pipeline conserve les matchs, et rel.economy_round est renseignée uniquement lorsqu’il existe une correspondance (match/map).

### 7.4 Requêtes d’analyse (exemples)

Top 10 maps les plus jouées

```bash
docker compose exec postgres psql -U csgo -d csgo -c "
SELECT m.map_name, COUNT(*) AS maps_played
FROM rel.match_map mm
JOIN rel.map m ON m.map_id = mm.map_id
GROUP BY m.map_name
ORDER BY maps_played DESC
LIMIT 10;"
```

Top joueurs par rating moyen (min 30 maps)

```bash
docker compose exec postgres psql -U csgo -d csgo -c "
SELECT p.player_name, COUNT(*) AS maps_played, ROUND(AVG(pm.rating)::numeric, 3) AS avg_rating
FROM rel.player_map pm
JOIN rel.player p ON p.player_id = pm.player_id
WHERE pm.rating IS NOT NULL
GROUP BY p.player_name
HAVING COUNT(*) >= 30
ORDER BY avg_rating DESC
LIMIT 10;"
```

## 8. Adminer (preuve visuelle)

URL : http://localhost:8080

Connexion :

Système : PostgreSQL

Serveur : postgres

Utilisateur : csgo

Mot de passe : valeur de .env

Base : csgo


## 9. Atelier 2 — Modèle dimensionnel (schéma `dwh`)

Cette partie correspond au **modèle orienté BI** (schéma en étoile) : **dimensions** + **faits**, alimentés à partir du modèle relationnel `rel`.


### 9.1 Schéma étoile (aperçu)


```mermaid
erDiagram
  DIM_DATE {
    int date_sk PK
    date full_date
  }

  DIM_EVENT {
    int event_id PK
    text event_name
  }

  DIM_MAP {
    int map_id PK
    text map_name
  }

  DIM_TEAM {
    int team_id PK
    text team_name
  }

  DIM_PLAYER {
    int player_id PK
    text player_name
    text country
  }

  FACT_TEAM_MAP_RESULT {
    int match_id PK
    int map_id FK
    int team_id PK
    int opponent_id FK
    int event_id FK
    int date_sk FK
    int score_for
    int score_against
    int round_diff
    boolean is_winner
  }

  FACT_PLAYER_MAP_STAR {
    int match_id PK
    int map_id FK
    int player_id PK
    int team_id FK
    int event_id FK
    int date_sk FK
    int kills
    int assists
    int deaths
    numeric kast
    numeric adr
    numeric rating
  }

  DIM_DATE   ||--o{ FACT_TEAM_MAP_RESULT   : date
  DIM_EVENT  ||--o{ FACT_TEAM_MAP_RESULT   : event
  DIM_MAP    ||--o{ FACT_TEAM_MAP_RESULT   : map
  DIM_TEAM   ||--o{ FACT_TEAM_MAP_RESULT   : team
  DIM_TEAM   ||--o{ FACT_TEAM_MAP_RESULT   : opponent

  DIM_DATE   ||--o{ FACT_PLAYER_MAP_STAR   : date
  DIM_EVENT  ||--o{ FACT_PLAYER_MAP_STAR   : event
  DIM_MAP    ||--o{ FACT_PLAYER_MAP_STAR   : map
  DIM_TEAM   ||--o{ FACT_PLAYER_MAP_STAR   : team
  DIM_PLAYER ||--o{ FACT_PLAYER_MAP_STAR   : player
```

![Schéma étoile (DWH)](docs/diagrams/STAR_DWH.png)



### 9.2 Tables DWH (résumé)

**Dimensions**
- `dwh.dim_date` (1 437 lignes)
- `dwh.dim_player` (12 295 lignes)
- `dwh.dim_team` / `dwh.dim_map` / `dwh.dim_event`

**Faits**
- `dwh.fact_team_map_result` (**60 710** lignes) : grain = 1 ligne par **équipe** et par **map** d’un match.
- `dwh.fact_player_map_star` (**295 023** lignes) : grain = 1 ligne par **joueur** et par **map** d’un match.

### 9.3 Exécution du script DWH

Script : `scripts/transform_to_dwh.sql`

```bash
docker compose run --rm etl bash -lc "apt-get update >/dev/null 2>&1 && apt-get install -y postgresql-client >/dev/null 2>&1 && PGPASSWORD=\$POSTGRES_PASSWORD psql -h postgres -U \$POSTGRES_USER -d \$POSTGRES_DB -f scripts/transform_to_dwh.sql"
```

### 9.4 Requêtes BI

Les requêtes BI complètes sont disponibles dans : `docs/sql/bi_queries.sql`.
Les captures des résultats de requètes sont dans `docs/diagrams/` (ex : `BI_Q1.png`).


**Q1 — Requête BI**

```sql
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
```

**Résultat** : `docs/diagrams/BI_Q1.png`

![Résultat Q1](docs/diagrams/BI_Q1.png)

**Q2 — Requête BI**

```sql

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
```

**Résultat (capture à ajouter)** : `docs/diagrams/BI_Q2.png`

![Résultat Q2](docs/diagrams/BI_Q2.png)

**Q3 — Requête BI**

```sql

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
```

**Résultat (capture à ajouter)** : `docs/diagrams/BI_Q3.png`

![Résultat Q3](docs/diagrams/BI_Q3.png)

## 10. Reset / nettoyage

```bash
docker compose down -v
```
## Atelier 3 — BI (Metabase) & Logs (Dozzle)

### 1) Metabase — connexion à Postgres

Metabase : `http://localhost:3000`

Ajouter une base de données (**PostgreSQL**) avec :

- Host : `postgres`
- Port : `5432`
- Database : `csgo`
- Username : `csgo`
- Password : valeur de `.env`

### 2) KPI (Q1 → Q5)

Créer 5 « questions » Metabase (requêtes SQL natives) à partir du schéma `dwh`.

Les requêtes sont dans :

- `docs/sql/bi_queries.sql`

Captures attendues (Metabase) :

- `docs/diagrams/A3_METABASE_KPI1.png`
- `docs/diagrams/A3_METABASE_KPI2.png`
- `docs/diagrams/A3_METABASE_KPI3.png`
- `docs/diagrams/A3_METABASE_KPI4.png`
- `docs/diagrams/A3_METABASE_KPI5.png`

### 3) Dashboard (preuve BI)

Créer un dashboard :

- **+ Nouveau** → **Dashboard**
- Nom : **A3 - Dashboard CSGO**
- Ajouter les 5 KPI et organiser les tuiles

📸 Capture : `docs/diagrams/A3_METABASE_DASHBOARD.png`

### 4) Dozzle — preuve d’erreur DB (logs)

Dozzle : `http://localhost:9999`

Objectif : montrer qu’une panne DB est visible en temps réel dans les logs Metabase.

1) Stop Postgres :

```bash
docker compose stop postgres
```

2) Recharger le dashboard / relancer un KPI (les requêtes échouent)

3) Dans Dozzle, ouvrir les logs du conteneur **metabase** et capturer une erreur de connexion, par ex. :

- `ERROR ... The connection attempt failed`
- `org.postgresql.util.PSQLException`

📸 Capture : `docs/diagrams/A3_DOZZLE_DB_ERROR.png`

4) Redémarrer Postgres :

```bash
docker compose start postgres
```

---

## Livrables attendus

### SQL

- `docs/sql/bi_queries.sql` : requêtes KPI Metabase (Q1 → Q5)

### Captures / schémas

- `docs/diagrams/MCD.PNG`
- `docs/diagrams/ERD_REL.png`
- `docs/diagrams/STAR_DWH.png`
- `docs/diagrams/BI_Q1.png`
- `docs/diagrams/BI_Q2.png`
- `docs/diagrams/BI_Q3.png`
- `docs/diagrams/A3_METABASE_DASHBOARD.png`
- `docs/diagrams/A3_DOZZLE_DB_ERROR.png`

---

## Reset / Nettoyage

```bash
docker compose down -v
```
