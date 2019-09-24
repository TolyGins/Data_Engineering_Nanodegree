import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA_PATH   = config.get('S3', 'LOG_DATA_PATH')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA_PATH  = config.get('S3', 'SONG_DATA_PATH')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stage_events(
artist varchar
, auth varchar
, firstName varchar
, gender varchar
, itemInSession varchar
, lastName varchar
, length varchar DISTKEY
, level varchar
, location varchar
, method varchar 
, page varchar
, registration varchar
, sessionId varchar
, song varchar
, status varchar
, ts varchar SORTKEY
, userAgent varchar
, userId varchar
)

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stage_songs(
artist_id varchar
, artist_latitude varchar
, artist_location varchar
, artist_longitude varchar
, artist_name varchar
, duration varchar  DISTKEY
, num_songs varchar
, song_id varchar
, title varchar 
, year varchar


)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
songplay_id int IDENTITY(0,1)
, start_time timestamp NOT NULL SORTKEY
, user_id int 
, level varchar NOT NULL
, song_id varchar NOT NULL DISTKEY
, artist_id varchar NOT NULL
, session_id varchar NOT NULL
, location varchar NOT NULL
, user_agent varchar NOT NULL 
, PRIMARY KEY (songplay_id)
)


""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
user_id int  SORTKEY
, first_name  varchar NOT NULL
, last_name  varchar NOT NULL
, gender  varchar NOT NULL
, level varchar  NOT NULL
, PRIMARY KEY (user_id)
)
DISTSTYLE ALL
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
song_id varchar NOT NULL DISTKEY
, title varchar NOT NULL
, artist_id varchar NOT NULL SORTKEY
, year int NOT NULL
, duration float NOT NULL
, PRIMARY KEY (song_id)
)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
artist_id varchar SORTKEY
, name varchar 
, location varchar
, latitude float
, longitude float
, PRIMARY KEY (artist_id)
)
DISTSTYLE ALL
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
start_time timestamp SORTKEY
, hour int
, day int
, week int
, month int
, year int
, weekday int
, PRIMARY KEY (start_time)
)
DISTSTYLE EVEN
""")

# STAGING TABLES

staging_events_copy = ("""
COPY stage_events 
FROM {} 
credentials 'aws_iam_role={}'
format as json {}
STATUPDATE ON
region 'us-west-2'
""").format(LOG_DATA_PATH, ARN, LOG_JSONPATH)


staging_songs_copy = ("""
COPY stage_songs
FROM {}
credentials 'aws_iam_role={}'
format as json 'auto'
STATUPDATE ON
region 'us-west-2'
""").format(SONG_DATA_PATH, ARN)



# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id,level,song_id, artist_id, session_id,location,user_agent)
SELECT
TIMESTAMP 'epoch' + CAST (ts AS bigint)/1000 * INTERVAL '1 second' AS start_time 
, CAST (userId AS int) AS user_id
, level
, song_id
, artist_id
, sessionId AS session_id
, location
, userAgent AS user_agent
FROM stage_events as main
INNER JOIN stage_songs AS s
    ON main.artist = s.artist_name 
    AND
    main.song = s.title
    AND
    CAST (s.duration AS float) = CAST (main.length AS float)
WHERE 
main.page = 'NextSong'
AND userId IS NOT NULL
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT 
--case when userId ~ '^[0–9]+$' then CAST (userid AS INT) else null end AS user_id 
 CAST (userId AS int) AS user_id
, firstName AS first_name
, lastName AS last_name
, gender
, level
FROM stage_events
WHERE 
page = 'NextSong'
AND userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT
song_id 
, title
, artist_id
, CAST (year AS int) AS year
, CAST (duration AS float) AS duration
FROM stage_songs
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
artist_id
, artist_name AS name
, artist_location AS location
, CAST (artist_latitude AS float) AS latitude 
, CAST (artist_longitude AS float) AS longitude
FROM stage_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
, DATE_PART('h', start_time) AS hour
, DATE_PART('d', start_time) AS day
, DATE_PART('w', start_time) AS week
, DATE_PART('mon', start_time) AS month
, DATE_PART('y', start_time) AS year
, EXTRACT ('weekday' from start_time) AS weekday
FROM stage_events
WHERE
page = 'NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
