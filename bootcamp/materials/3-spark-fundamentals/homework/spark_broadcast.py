from pyspark.sql import SparkSession
from pyspark.sql import functions as f
spark = SparkSession.builder.appName("Jupyter").getOrCreate() #Spark is a JVM lib so we have camel case instead of snake case

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# Load medals
medals = spark.read.option(
    "header", "true"
).csv(
    "/home/iceberg/data/medals.csv"
)

# Load maps
maps = spark.read.option(
    "header", "true"
).csv(
    "/home/iceberg/data/maps.csv"
)

# Load matches
matches = spark.read.option(
    "header", "true"
).csv(
    "/home/iceberg/data/matches.csv"
)

# Load medaL_matches_players
medals_matches_players = spark.read.option(
    "header", "true"
).csv(
    "/home/iceberg/data/medals_matches_players.csv"
)

# Medals and Maps should have a common identifier, we will use match_id
medals = medals.join(
    medals_matches_players,
    on='medal_id',
    how='inner'
).select(
    medals['*'],
    medals_matches_players.match_id
).dropDuplicates()

maps = maps.join(
    matches,
    on='mapid',
    how='inner'
).select(
    maps['*'],
    matches.match_id
).dropDuplicates()


df_join = medals.join(
    f.broadcast(maps),
    on='match_id',
    how='inner'
).select(
    medals.medal_id,
    medals.sprite_uri,
    medals.sprite_left,
    medals.sprite_top,
    medals.sprite_sheet_width,
    medals.sprite_sheet_height,
    medals.sprite_width,
    medals.sprite_height,
    medals.classification,
    medals.description,
    medals.name,
    medals.difficulty,
    maps.mapid,
    maps.name,
    maps.description
).dropDuplicates()