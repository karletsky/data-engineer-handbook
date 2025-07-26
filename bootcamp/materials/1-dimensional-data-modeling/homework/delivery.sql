-- Exercise 1
CREATE TYPE film_struct AS (
	year INTEGER,
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);

CREATE TYPE quality_class AS
	ENUM('star','good','average','bad');

DROP TABLE actors;
CREATE TABLE actors (
	actor TEXT,
	films film_struct[],
	quality_class quality_class,
	is_active BOOLEAN,
	current_year INTEGER,
	PRIMARY KEY(actor,current_year)
);

-- Exercise 2
INSERT INTO actors
WITH years AS (
	SELECT *
	    FROM GENERATE_SERIES(1970, 2021) AS year
), p AS (
	SELECT
        actor,
        MIN(year) AS first_year
    FROM actor_films
    GROUP BY actor
), actors_and_years AS (
	SELECT *
    FROM p
    JOIN years y
        ON p.first_year <= y.year
		
), windowed AS (
	SELECT
		aay.actor,
		aay.year,
		ARRAY_REMOVE(
			ARRAY_AGG(
				CASE
					WHEN af.year IS NOT NULL THEN 
					ROW(
						af.year,
						af.film,
						af.votes,
						af.rating,
						af.filmid
					)::film_struct
			    END)
			OVER (PARTITION BY aay.actor ORDER BY COALESCE(aay.year,af.year)),
			NULL
			) as films
		    FROM actors_and_years aay
			LEFT JOIN actor_films af
				ON aay.actor = af.actor
				AND aay.year = af.year
			ORDER BY aay.actor, aay.year	
), ranked AS (
	SELECT
		actor,
		films,
		year,
		ROW_NUMBER() OVER (PARTITION BY actor,year ORDER BY CARDINALITY(films) DESC) as rn
	FROM windowed
), yearly_ratings AS (
	SELECT
		actor,
		year,
		AVG(rating) as avg_rating
	FROM actor_films af
	GROUP BY actor,year
)
SELECT
	r.actor,
	r.films,
	CASE
		WHEN yr.avg_rating > 8.0 THEN 'star'
		WHEN yr.avg_rating > 7.0 THEN 'good'
		WHEN yr.avg_rating > 6.0 THEN 'average'
		ELSE 'bad'
	END::quality_class AS quality_class,
	(films[CARDINALITY(films)]::film_struct).year = r.year AS is_active,
	r.year
FROM ranked r
LEFT JOIN yearly_ratings yr  -- Join yearly averages
    ON r.actor = yr.actor
    AND r.year = yr.year
WHERE rn = 1

-- Exercise 3
CREATE TABLE actors_history_scd (
	actor TEXT,
	quality_class quality_class,
	is_active BOOLEAN,
	start_date INTEGER,
	end_date INTEGER,
	current_year INTEGER,
	PRIMARY KEY (actor, start_date)
);
-- Exercise 4
INSERT INTO actors_history_scd
WITH with_previous AS (
SELECT
	actor,
	current_year,
	quality_class,
	is_active,
	LAG(quality_class,1) OVER (PARTITION BY actor ORDER BY current_year) as previous_quality_class,
	LAG(is_active,1) OVER (PARTITION BY actor ORDER BY current_year) as previous_is_active
FROM actors
WHERE current_year <= 2020
), with_indicators AS (
	SELECT
		*,
		CASE
			WHEN quality_class <> previous_quality_class THEN 1
			WHEN is_active <> previous_is_active THEN 1
			ELSE 0 
			END AS change_indicator
	FROM with_previous
), with_streaks AS (
SELECT
	*,
	SUM(change_indicator)  OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
FROM with_indicators
)

SELECT
	actor,
	--streak_identifier,
	quality_class,
	is_active,
	MIN(current_year) as start_date,
	MAX(current_year) as end_date,
	2020 as current_year
FROM with_streaks
GROUP BY actor,streak_identifier,is_active,quality_class
ORDER BY actor,streak_identifier

-- Exercise 5
CREATE TYPE SCD_TYPE AS (
	quality_class quality_class,
	is_active BOOLEAN,
	start_date INTEGER,
	end_DATE INTEGER
)
WITH last_year_scd AS (
	SELECT * FROM actors_history_scd
	WHERE current_year = 2020
	AND end_date = 2020
),
	historical_scd AS (
		SELECT
			actor,
			quality_class,
			is_active,
			start_date,
			end_date
		FROM actors_history_scd
		WHERE current_year = 2020
		AND end_date < 2020
), this_year_data AS (
	SELECT * FROM actors
	WHERE current_year = 2021
), unchanged_records AS (
	SELECT
		ty.actor,
		ty.quality_class,
		ty.is_active,
		ly.start_date,
		ty.current_year as end_date
	FROM this_year_data ty
	LEFT JOIN last_year_scd ly
	ON ly.actor = ty.actor
	WHERE ty.quality_class = ly.quality_class
	AND ty.is_active = ly.is_active
), changed_records AS (
	SELECT
		ty.actor,
		UNNEST(ARRAY[
			ROW(
				ly.quality_class,
				ly.is_active,
				ly.start_date,
				ly.end_date
			)::scd_type,
			ROW(
				ty.quality_class,
				ty.is_active,
				ty.current_year,
				ty.current_year
			)::scd_type
		]) as records
	FROM this_year_data ty
	LEFT JOIN last_year_scd ly
	ON ly.actor = ty.actor
	WHERE (ty.quality_class <> ly.quality_class
	OR ty.is_active = ly.is_active)
), unnested_changed_records AS (
	SELECT
		actor,
		(records::scd_type).quality_class,
		(records::scd_type).is_active,
		(records::scd_type).start_date,
		(records::scd_type).end_date
	FROM changed_records	
),  new_records AS (
	SELECT
		ty.actor,
		ty.quality_class,
		ty.is_active,
		ty.current_year AS start_date,
		ty.current_year AS end_date
	FROM this_year_data ty
	LEFT JOIN last_year_scd ly
	ON ty.actor = ly.actor
	WHERE ly.actor IS NULL
)
SELECT * FROM historical_scd
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records

