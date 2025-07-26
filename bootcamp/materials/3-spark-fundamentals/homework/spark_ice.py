from pyspark.sql import SparkSession
from pyspark.sql import functions as f
spark = SparkSession.builder.appName("Jupyter").getOrCreate() #Spark is a JVM lib so we have camel case instead of snake case
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# Load matches
matches = spark.read.option(
    "header", "true"
).csv(
    "/home/iceberg/data/matches.csv"
).select(
    'match_id',
    'is_team_game',
    'playlist_id',
    'mapid',
    'completion_date'
)

# Bucketed matches
spark.sql("""DROP TABLE IF EXISTS bootcamp.hw2_matches_bucketed""")
ddl_query_matches = """
CREATE TABLE IF NOT EXISTS bootcamp.hw2_matches_bucketed (
     match_id STRING,
     is_team_game BOOLEAN,
     playlist_id STRING,
     mapid STRING,
     completion_date TIMESTAMP
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(ddl_query_matches)

matches = matches.withColumn(
    'is_team_game',
    f.when(f.col('is_team_game')=='true',True).otherwise(False)
)

matches = matches.withColumn(
    'completion_date',
    f.to_timestamp("completion_date", "yyyy-MM-dd HH:mm:ss.SSSSSS")
)

matches = matches.sortWithinPartitions(f.col("completion_date"), f.col("playlist_id"), f.col('mapid')) # Lowest cardinality to the right

matches.write.mode(
    'overwrite'
).bucketBy(
    16,
    'match_id'
).saveAsTable('bootcamp.hw2_matches_bucketed')

df_files = spark.sql("""
    SELECT * FROM bootcamp.hw2_matches_bucketed.files
""")

# Before using sortWithinPartitions was 606498

df_files.select(f.sum('file_size_in_bytes')).alias('sum').show() 

# Load match_details
match_details = spark.read.option(
    "header", "true"
).csv(
    "/home/iceberg/data/match_details.csv"
).select(
    'match_id',
    'player_gamertag',
    'player_total_kills',
    'player_total_deaths'
)

# Bucketed match_details
spark.sql("""DROP TABLE IF EXISTS bootcamp.hw2_match_details_bucketed""")
ddl_query_match_details = """
CREATE TABLE IF NOT EXISTS bootcamp.hw2_match_details_bucketed (
    match_id STRING,
    player_gamertag STRING,
    player_total_kills INTEGER,
    player_total_deaths INTEGER
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(ddl_query_match_details)

match_details.write.mode(
    'overwrite'
).bucketBy(
    16,
    'match_id'
).saveAsTable('bootcamp.hw2_match_details_bucketed')

match_details.unpersist()

# Load medaL_matches_players
medals_matches_players = spark.read.option(
    "header", "true"
).csv(
    "/home/iceberg/data/medals_matches_players.csv"
)

# Bucketed medal_matches_players
spark.sql("""DROP TABLE IF EXISTS bootcamp.hw2_medal_matches_players""")
ddl_query_match_details = """
CREATE TABLE IF NOT EXISTS bootcamp.hw2_medal_matches_players (
    match_id STRING,
    player_gamertag STRING,
    medal_id STRING,
    count STRING
)
USING iceberg
PARTITIONED BY (bucket(16, match_id));
"""
spark.sql(ddl_query_match_details)

medals_matches_players.write.mode(
    'overwrite'
).bucketBy(
    16,
    'match_id'
).saveAsTable('bootcamp.hw2_medal_matches_players')

medals_matches_players.unpersist()

df_results = spark.sql("""
    SELECT
        m.match_id,
        m.is_team_game,
        m.playlist_id,
        m.completion_date,
        m.mapid,
        md.player_gamertag,
        md.player_total_kills,
        md.player_total_deaths,
        mm.player_gamertag AS medal_player_gametag,
        mm.medal_id,
        mm.count
    FROM bootcamp.hw2_matches_bucketed m
    INNER JOIN bootcamp.hw2_match_details_bucketed md
    ON m.match_id = md.match_id
    INNER JOIN bootcamp.hw2_medal_matches_players mm
    ON m.match_id = mm.match_id
""")

# Questions
# Which player averages the most kills per game? gimpinator14
df_kills = df_results.select(
    'match_id',
    'is_team_game',
    'completion_date',
    'player_gamertag',
    'player_total_kills',
    'player_total_deaths'
).dropDuplicates(
).groupBy(
    'player_gamertag'
).agg(
    f.avg(f.col('player_total_kills')).alias('avg_kills')
).orderBy(
    'avg_kills',
    ascending=False
).limit(1)
print(df_kills.show())

# Which playlist gets played the most? f72e0ef0-7c4a-4307-af78-8e38dac3fdba
df_playlists = df_results.select(
    'playlist_id',
    'match_id',
).dropDuplicates(
).groupBy(
    'playlist_id'
).count(
).orderBy(
    'count',
    ascending=False
).limit(1)

print(df_playlists.show(truncate=False))

# Which map gets played the most?
# Which playlist gets played the most? f72e0ef0-7c4a-4307-af78-8e38dac3fdba
df_maps = df_results.select(
    'mapid',
    'match_id',
).dropDuplicates(
).groupBy(
    'mapid'
).count(
).orderBy(
    'count',
    ascending=False
).limit(1)

print(df_maps.show(truncate=False))

# Which map do players get the most Killing Spree medals on?
# Load medals
medals = spark.read.option(
    "header", "true"
).csv(
    "/home/iceberg/data/medals.csv"
).filter(
    f.col('name').like('%Killing Spree%')
)

df_kp = df_results.select(
    'mapid',
    'medal_id',
    'count'
).join(
    medals,
    on='medal_id',
    how='inner'
).groupBy(
    'mapid'
).agg(
    f.sum('count').alias('medal_count')
).orderBy(
    'medal_count',
    ascending=False
).limit(1)

print(df_kp.show(truncate=False))