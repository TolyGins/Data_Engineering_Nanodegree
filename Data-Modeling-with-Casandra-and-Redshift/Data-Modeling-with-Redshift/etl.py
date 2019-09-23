import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import json


def process_song_file(cur, filepath):
    """This function handles the song files, opening, filtering and inserting the data into our song and artist tables 
       
    Args:
        cur (psycopg2.cur): This is the curser for the db connection
        filepath (str): The filepath where the data lives

    Returns:
        executes sql statements related to song file and song data tables."""
    
    
    # open song file
    try:
        with open(filepath, 'r') as f:
            data = [json.load(f)]
        df = pd.DataFrame(data)
    except Exception as e:
        print("Error converting {} to dataframe".format(filepath))

    # insert song record
    try:
        song_data = list(df.iloc[0][['song_id', 'title', 'artist_id', 'year', 'duration']].values)
        try:
            song_data[-2] = int(song_data[-2])
        except Exception as e:
            print("Error converting year into int from numpy int64")
        cur.execute(song_table_insert, song_data)
    except Exception as e:
        print (e)
        print("Error inserting song data into song table using file {}".format(filepath))

    # insert artist record
    try:
        artist_data = list(df.iloc[0][['artist_id', 'artist_name',
                                       'artist_location', 'artist_latitude', 'artist_longitude']].values)
        cur.execute(artist_table_insert, artist_data)
    except Exception as e:
        print (e)
        print("Error inserting artist data into artist table using file {}".format(filepath))


def process_log_file(cur, filepath):
    """This function handles the log files. It opens, reads each line, transforms the data and bulk inserts it into our tables
       
    Args:
        cur (psycopg2.cur): This is the curser for the db connection
        filepath (str): The filepath where the data lives

    Returns:
        executes sql queries for the FACT and DIM tables."""
    
    # open log file
    json_list = []
    try:
        for line in open(filepath, 'r'):
            data = json.loads(line)
            json_list.append(data)
        df = pd.DataFrame(json_list)
    except Exception as e:
        print('Error loading log file {} into json '.format(filepath))

    # filter by NextSong action
    next_song_df = df[(df.page == 'NextSong')]

    # convert timestamp column to datetime
    next_song_df['ts'] = pd.to_datetime(next_song_df['ts'], unit='ms')

    # insert time data records
    time_data = [next_song_df.ts, next_song_df.ts.dt.hour, next_song_df.ts.dt.day, next_song_df.ts.dt.week, next_song_df.ts.dt.month,
                 next_song_df.ts.dt.year, next_song_df.ts.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    # convert to df
    new_dict = {}
    for values, label in zip(time_data, column_labels):
        new_dict[label] = values
    time_df = pd.DataFrame(new_dict)

    # insert multiple values into time table
    try:
        time_data = list(time_df.itertuples(index=False))
        args_str_time = ','.join(cur.mogrify("(%s,%s,%s,%s,%s,%s,%s)",
                                             each).decode("utf-8") for each in time_data)
        cur.execute(time_table_insert + args_str_time+'ON CONFLICT (start_time) DO NOTHING')
    except Exception as e:
        
        print('Error with inserting {} file into time table'.format(filepath))

    # load user table
    user_df = next_song_df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    try:
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)
            
    except Exception as e:
        print (e)
        #print('Error with inserting {} file into user table'.format(filepath))

    # insert songplay record
    try:
        for index, row in next_song_df.iterrows():
            # get songid and artistid from song and artist tables
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
            if results:
                songid, artistid = results
            else:
                songid, artistid = None, None
            # insert songplay record
            songplay_data = (row.ts, row.userId, row.level, songid, artistid,
                             row.sessionId, row.location, row.userAgent)
            songplay_data = (row.ts, row.userId, row.level, str(songid), str(
                artistid), row.sessionId, row.location, row.userAgent)
            cur.execute(songplay_table_insert, songplay_data)
    except Exception as e:
        print('Error with inserting {} file into songplay table'.format(filepath))


def process_data(cur, conn, filepath, func):
    """This function scans through all the folders in our directory and appends the files to a list
    
    Args:
        cur: the cursor to the db
        conn: the connection to the db
        filepath: the filepath where song and log data live
        func: the function that processes data and runs sql that you want to use
    
    Returns:
        Iterates through all subfolders and files and prints the processed file names"""
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
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
    """This function establishes a db connection and runs the rest of our functions"""
    
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
