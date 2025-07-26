from pyspark.sql import SparkSession

query = """
WITH with_previous AS (
  SELECT
    actor,
    current_year,
    quality_class,
    is_active,
    LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) AS previous_quality_class,
    LAG(is_active, 1) OVER (PARTITION BY actor ORDER BY current_year) AS previous_is_active
  FROM actors
  WHERE current_year <= 2020
),
with_indicators AS (
  SELECT
    *,
    CASE
      WHEN quality_class <> previous_quality_class THEN 1
      WHEN is_active <> previous_is_active THEN 1
      ELSE 0
    END AS change_indicator
  FROM with_previous
),
with_streaks AS (
  SELECT
    *,
    SUM(change_indicator) OVER (PARTITION BY actor ORDER BY current_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS streak_identifier
  FROM with_indicators
)
SELECT
  actor,
  quality_class,
  is_active,
  MIN(current_year) AS start_date,
  MAX(current_year) AS end_date,
  2020 AS current_year
FROM with_streaks
GROUP BY actor, streak_identifier, is_active, quality_class
ORDER BY actor, streak_identifier
"""

def do_actor_scd_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("actors")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_scd") \
      .getOrCreate()
    output_df = do_actor_scd_transformation(spark, spark.table("actors"))
    output_df.write.mode("overwrite").insertInto("actors_scd")

