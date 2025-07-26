-- 1. Dedup query
WITH ranked AS (
	SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY game_id,team_id,player_id) as rn
	FROM game_details
	)
SELECT
	*
FROM ranked
WHERE rn = 1

--2. DDL user_devices_cumulated
CREATE TABLE user_devices_cumulated (
	user_id TEXT,
	browser_type TEXT,
	device_activity_datelist DATE[],
	date DATE, --current date
	PRIMARY KEY(user_id,browser_type,date)
)

--3. Cumulative query for user_devices_cumulated
INSERT INTO user_devices_cumulated
WITH yesterday AS (
	SELECT
		*
	FROM user_devices_cumulated
	WHERE date = date('2023-01-30')
), today AS (
	SELECT
		CAST(e.user_id AS TEXT) AS user_id, 
		d.browser_type,
		DATE(CAST(e.event_time AS TIMESTAMP)) AS date_active
	FROM events e
	INNER JOIN devices d
	ON e.device_id = d.device_id
	WHERE DATE(CAST(e.event_time AS TIMESTAMP)) = DATE('2023-01-31')
	AND e.user_id IS NOT NULL
	GROUP BY e.user_id, d.browser_type, date_active
)
SELECT
	COALESCE(t.user_id, y.user_id) as user_id,
	COALESCE(t.browser_type, y.browser_type) as browser_type,
	CASE 
		WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.device_activity_datelist
		ELSE ARRAY[t.date_active] || y.device_activity_datelist
	END
	AS device_activity_datelist,
	COALESCE(t.date_active, y.date + Interval '1 day') AS date
	
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
AND t.browser_type = y.browser_type;

--4. Convert device_activity_datelist to datelist_int
WITH users AS (
	SELECT * FROM user_devices_cumulated
	WHERE date = DATE('2023-01-31')
),
	series AS (
	SELECT * 
	FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day')
		as series_date
), place_holder_ints AS (
	SELECT
		CASE 
			WHEN device_activity_datelist @> ARRAY[DATE(series_date)] THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
		ELSE 0
		END AS placeholder_int_value,
		*
	FROM users
	CROSS JOIN series
)
SELECT
	user_id,
	browser_type,
	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int
FROM place_holder_ints
GROUP BY user_id, browser_type

--5. DDL for hosts cumulated
CREATE TABLE hosts_cumulated (
	host TEXT,
	host_activity_datelist DATE[],
	date DATE, --current date
	PRIMARY KEY(host,date)
)

--6. Hosts activity datelist
INSERT INTO hosts_cumulated
WITH yesterday AS (
	SELECT
		*
	FROM hosts_cumulated
	WHERE date = date('2023-01-30')
), today AS (
	SELECT
		host,
		DATE(CAST(e.event_time AS TIMESTAMP)) AS date_active
	FROM events e
	WHERE DATE(CAST(e.event_time AS TIMESTAMP)) = DATE('2023-01-31')
	AND e.host IS NOT NULL
	GROUP BY host, date_active
)
SELECT
	COALESCE(t.host,y.host),
	CASE
		WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.host_activity_datelist
		ELSE ARRAY[t.date_active] || y.host_activity_datelist
	END
	AS host_activity_datelist,
	DATE(COALESCE(t.date_active, y.date + Interval '1 day')) AS date
FROM today t
FULL OUTER JOIN yesterday y
ON t.host = y.host;
SELECT * FROM hosts_cumulated
ORDER BY date DESC

--7. Reduced fact table
CREATE TABLE host_activity_reduced (
		host TEXT,
		month_start DATE,
		hit_array INTEGER[],
		unique_visitors_array INTEGER[],
		PRIMARY KEY (host, month_start)
)
--8. Incremental load query
INSERT INTO host_activity_reduced
WITH daily_aggregate AS (
	SELECT
		host,
		DATE(event_time) AS date,
		COUNT(1) AS num_site_hits,
		COUNT(DISTINCT user_id) unique_visitors
	FROM events
	WHERE DATE(event_time) = DATE('2023-01-03')
	GROUP BY host, date
),	yesterday_array AS (
	SELECT
		*
	FROM host_activity_reduced
	WHERE month_start = DATE('2023-01-01')
)

SELECT
	COALESCE(da.host,ya.host) AS host,
	COALESCE(ya.month_start, DATE_TRUNC('month',da.date)) AS month_start,
	CASE 
		WHEN ya.hit_array IS NOT NULL THEN ya.hit_array || ARRAY[COALESCE(da.num_site_hits,0)]
		WHEN ya.hit_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(date - month_start,0)]) || ARRAY[COALESCE(da.num_site_hits,0)]
	END as hit_array,
	CASE 
		WHEN ya.unique_visitors_array IS NOT NULL THEN ya.unique_visitors_array || ARRAY[COALESCE(da.unique_visitors,0)]
		WHEN ya.unique_visitors_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(date - month_start,0)]) || ARRAY[COALESCE(da.unique_visitors,0)]
	END as unique_visitors_array
FROM daily_aggregate da
FULL OUTER JOIN yesterday_array ya
ON da.host = ya.host
ON CONFLICT (host, month_start)
DO UPDATE
	SET hit_array = EXCLUDED.hit_array, unique_visitors_array = EXCLUDED.unique_visitors_array;



