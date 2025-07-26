--Query 1: Track Player's State Changes
WITH yesterday AS (
	SELECT *
	FROM players
	WHERE current_season = 1997
),
	today AS (
	SELECT
		player_name,
		is_active,
		current_season
	FROM players
	WHERE current_season = 1998
)

SELECT
	COALESCE(y.player_name,t.player_name) as player_name,
	CASE
		WHEN y.player_name IS NULL THEN 'New'
		WHEN y.is_active AND NOT t.is_active THEN 'Retired'
		WHEN y.is_active AND t.is_active THEN 'Continued Playing'
		WHEN NOT y.is_active AND t.is_active THEN 'Returned from Retirement'
		ELSE 'Stayed Retired'
	END AS season_state_tracking,
	t.current_season as current_season
FROM Today t
FULL OUTER JOIN Yesterday y
ON t.player_name=y.player_name

--Query 2: Aggregations Using Grouping Sets
SELECT
	COALESCE(gd.team_city,'(overall)'),
	COALESCE(gd.player_name,'(overall)'),
	COALESCE(CAST(g.season AS TEXT),'(overall)'),
	COALESCE(SUM(pts),0) as points
FROM game_details gd
INNER JOIN games g
ON gd.game_id = g.game_id
GROUP BY GROUPING SETS (
	(gd.player_name, gd.team_city),
	(gd.player_name, g.season),
	(gd.team_city)
);
--Query 3: who scored the most points playing for one team?
SELECT
	COALESCE(gd.team_city,'(overall)') as team_city,
	COALESCE(gd.player_name,'(overall)') as player_name,
	COALESCE(CAST(g.season AS TEXT),'(overall)') as season,
	COALESCE(SUM(pts),0) as points
FROM game_details gd
INNER JOIN games g
ON gd.game_id = g.game_id
GROUP BY GROUPING SETS (
	(gd.player_name, gd.team_city),
	(gd.player_name, g.season),
	(gd.team_city)
)
HAVING GROUPING(season) = 1
AND GROUPING(team_city) = 0
AND GROUPING(player_name) = 0
ORDER BY points DESC
LIMIT 1
--Query 4: who scored the most points in one season?
SELECT
	COALESCE(gd.team_city,'(overall)') as team_city,
	COALESCE(gd.player_name,'(overall)') as player_name,
	COALESCE(CAST(g.season AS TEXT),'(overall)') as season,
	COALESCE(SUM(pts),0) as points
FROM game_details gd
INNER JOIN games g
ON gd.game_id = g.game_id
GROUP BY GROUPING SETS (
	(gd.player_name, gd.team_city),
	(gd.player_name, g.season),
	(gd.team_city)
)
HAVING GROUPING(season) = 0
AND GROUPING(team_city) = 1
AND GROUPING(player_name) = 0
ORDER BY points DESC
LIMIT 1
--Query 5: team with most total wins
SELECT
	COALESCE(gd.team_city,'(overall)') as team_city,
	COALESCE(gd.player_name,'(overall)') as player_name,
	COALESCE(CAST(g.season AS TEXT),'(overall)') as season,
	COALESCE(SUM(pts),0) as points
FROM game_details gd
INNER JOIN games g
ON gd.game_id = g.game_id
GROUP BY GROUPING SETS (
	(gd.player_name, gd.team_city),
	(gd.player_name, g.season),
	(gd.team_city)
)
HAVING GROUPING(season) = 1
AND GROUPING(team_city) = 0
AND GROUPING(player_name) = 1
ORDER BY points DESC
LIMIT 1
--Query 6: What is the most games a team has won in a 90 game stretch?
WITH games_filtered AS (
SELECT
	DISTINCT
	gd.game_id,
	gd.team_id,
	gd.team_city,
	g.home_team_id,
	g.visitor_team_id,
	g.home_team_wins,
	CASE
		WHEN team_id = home_team_id AND home_team_wins = 1 THEN 1
		WHEN team_id = visitor_team_id AND home_team_wins = 0 THEN 1
		ELSE 0
	END AS team_wins,
	ROW_NUMBER() OVER (PARTITION BY gd.team_id ORDER BY g.game_date_est) / 90 AS stretch
FROM game_details gd
INNER JOIN games g
ON gd.game_id = g.game_id
)

SELECT
	team_id,
	team_city,
	stretch,
	SUM(team_wins) as team_wins
FROM games_filtered
GROUP BY team_id,team_city,stretch

--Query 7: How many games in a row did LeBron James score over 10 points a game?

WITH filtered_games AS (
	SELECT 
		game_id,
		pts,
		CASE WHEN pts > 10 THEN 1 ELSE 0 END AS over_10_pts,
		ROW_NUMBER() OVER (ORDER BY game_id) - SUM(CASE WHEN pts > 10 THEN 1 ELSE 0 END) OVER (ORDER BY game_id) AS grp
	FROM game_details
	WHERE player_name LIKE '%LeBron James%'
),
streaks AS (
  SELECT
    grp,
    COUNT(*) AS streak_length
  FROM filtered_games
  WHERE over_10_pts = 1
  GROUP BY grp
)
SELECT MAX(streak_length) FROM streaks;




