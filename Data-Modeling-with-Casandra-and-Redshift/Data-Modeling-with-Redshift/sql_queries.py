# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplays (
songplay_id serial PRIMARY KEY
, start_time TIMESTAMP 
, user_id INT
, level VARCHAR 
, song_id VARCHAR
, artist_id VARCHAR 
, session_id VARCHAR
, location VARCHAR
, user_agent VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE users (
user_id INT PRIMARY KEY
, first_name varchar 
, last_name varchar
, gender varchar
, level varchar
)

""")

song_table_create = ("""
CREATE TABLE songs (
song_id VARCHAR NOT NULL PRIMARY KEY
, title VARCHAR NOT NULL 
, artist_id VARCHAR NOT NULL
, year INT
, duration FLOAT
)
""")

artist_table_create = ("""
CREATE TABLE artists (
artist_id VARCHAR NOT NULL PRIMARY KEY
, name VARCHAR NOT NULL 
, location VARCHAR NOT NULL
, latitude FLOAT 
, longitude FLOAT
)
""")

time_table_create = ("""
CREATE TABLE time (
start_time TIMESTAMP NOT NULL PRIMARY KEY
, hour INT NOT NULL
, day INT NOT NULL
, week INT NOT NULL
, month INT NOT NULL
, year INT NOT NULL
, weekday INT NOT NULL

)
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id,level,song_id, artist_id, session_id,location,user_agent) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)

""")

user_table_insert = ("""
INSERT INTO users VALUES (%s,%s,%s,%s,%s)

ON CONFLICT (user_id) DO UPDATE
SET level = EXCLUDED.level
""")



song_table_insert = ("""
INSERT INTO songs VALUES(%s, %s, %s, %s,%s)

ON CONFLICT (song_id) DO NOTHING
""")

artist_table_insert = ("""
INSERT INTO artists VALUES(%s, %s, %s, %s,%s)
 
ON CONFLICT (artist_id) DO NOTHING
""")


time_table_insert = ("""
INSERT INTO time VALUES


""")

# FIND SONGS

song_select = ("""
SELECT 
s.song_id
, s.artist_id
FROM songs as s
INNER JOIN artists AS art USING (artist_id)
WHERE
s.title = %s
AND art.name = %s
AND s.duration = %s

""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]