import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    reads song json data, extracts song and artist information and performs insert operations on songs and artists table
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = [df.song_id.tolist()[0], df.title.tolist()[0], df.artist_id.tolist()[0], df.year.tolist()[0], df.duration.tolist()[0]]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [df.artist_id.tolist()[0], df.artist_name.tolist()[0], df.artist_location.tolist()[0], df.artist_latitude.tolist()[0], df.artist_longitude.tolist()[0]]

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    reads song stream data, extracts time information (inster in time table), user information (insert in users table), and in songplays(table)
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"]=='NextSong']

    # convert timestamp column to datetime
#     t = pd.DataFrame()
    df["time_data"] = pd.to_datetime(df['ts'], unit='ms')
    df["hour"] = df["time_data"].apply(lambda x: x.hour)
    df["day"] = df["time_data"].apply(lambda x: x.day)
    df["week"] = df["time_data"].apply(lambda x: x.week)
    df["year"] = df["time_data"].apply(lambda x: x.year)
    df["month"] = df["time_data"].apply(lambda x: x.month)
    df["weekday"] = df["time_data"].apply(lambda x: x.weekday())
    
    # insert time data records
    time_data = (df['time_data'].tolist(), df['hour'].tolist(), df['day'].tolist(), df['week'].tolist(), df['month'].tolist(), df['year'].tolist(), df['weekday'].tolist())
    column_labels = ['time_data', 'hour', 'day', 'week', 'month', 'year', 'weekday'] 
    time_df = df[column_labels]

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.time_data, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    iterates over files in a directory and runs 'func' over the file
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()