# Data Modeling with Postgres

The project intends to model data for a music streaming app on postgres. The scripts create (or re-create) a database with 5 tables, following a star schema

1. songplay_table - this is the fact table that contains logs of  music streams. (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
2. user_table - a dimenstion table with user information. (user_id, first_name, last_name, gender, level)
3. song_table - a dimenstion table with song information. (song_id, title, artist_id, year, duration)
4. artist_table - a dimenstion table with artist information. (artist_id, name, location, latitude, longitude)
5. time_table - a dimenstion table with time information. (start_time, hour, day, week, month, year, weekday)


## To create tables
`python create_tables.py`

## To run ETL pipelines on the created tables
`python etl.py`


## To run sanity checks
run test.ipynb

