from pyspark.sql import SparkSession

query = """
WITH yesterday AS (
  SELECT *
  FROM user_devices_cumulated
  WHERE date = DATE('2023-01-30')
),
today AS (
  SELECT
    CAST(e.user_id AS STRING) AS user_id,
    d.browser_type,
    DATE(CAST(e.event_time AS TIMESTAMP)) AS date_active
  FROM events e
  INNER JOIN devices d
    ON e.device_id = d.device_id
  WHERE DATE(CAST(e.event_time AS TIMESTAMP)) = DATE('2023-01-31')
    AND e.user_id IS NOT NULL
  GROUP BY e.user_id, d.browser_type, DATE(CAST(e.event_time AS TIMESTAMP))
)
SELECT
  COALESCE(t.user_id, y.user_id) AS user_id,
  COALESCE(t.browser_type, y.browser_type) AS browser_type,
  CASE 
    WHEN y.device_activity_datelist IS NULL THEN ARRAY(t.date_active)
    WHEN t.date_active IS NULL THEN y.device_activity_datelist
    ELSE ARRAY(t.date_active) || y.device_activity_datelist
  END AS device_activity_datelist,
  COALESCE(t.date_active, DATE_ADD(y.date, 1)) AS date
FROM today t
FULL OUTER JOIN yesterday y
  ON t.user_id = y.user_id AND t.browser_type = y.browser_type;
"""

def do_user_devices_transformation(spark, dataframe):
    dataframe.createOrReplaceTempView("user_devices_temp")
    return spark.sql(query)


def main():
    spark = SparkSession.builder \
      .master("local") \
      .appName("actors_scd") \
      .getOrCreate()
    output_df = do_user_devices_transformation(spark, spark.table("user_devices_temp"))
    output_df.write.mode("overwrite").insertInto("user_devices")

