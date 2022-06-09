# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id int PRIMARY KEY NOT NULL, start_time TIMESTAMP  NOT NULL, user_id int NOT NULL, level varchar NOT NULL, song_id varchar, artist_id varchar, session_id int NOT NULL, location varchar NOT NULL, user_agent varchar NOT NULL);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (user_id int PRIMARY KEY, first_name varchar NOT NULL, last_name varchar NOT NULL, gender varchar, level varchar NOT NULL);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (song_id varchar PRIMARY KEY, title varchar NOT NULL, artist_id varchar NOT NULL, year int NOT NULL, duration float NOT NULL);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (artist_id varchar PRIMARY KEY, name varchar NOT NULL, location varchar NOT NULL, latitude float NOT NULL, longitude float NOT NULL);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP NOT NULL, hour int NOT NULL, day int, week int, month int, year int, weekday int);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (songplay_id) DO NOTHING
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) DO 
    UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, gender = EXCLUDED.gender, level = EXCLUDED.level

""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) DO 
    UPDATE SET song_id = EXCLUDED.song_id, artist_id = EXCLUDED.artist_id, year = EXCLUDED.year, duration = EXCLUDED.duration

""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) DO 
    UPDATE SET name=EXCLUDED.name, location = EXCLUDED.location, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude
""")


time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
""")

# FIND SONGS

song_select = ("""
SELECT * FROM songs WHERE song_id=%s AND artist_id=%s AND duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]