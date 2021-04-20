import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

#load data from a song_data to song, artist tbls
def process_song_file(cur, conn, filepath):
    all_files=[]
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    print('{} files are contained in "{}"'.format(len(all_files), filepath)) 
    for i in range(len(all_files)):
        df=pd.read_json(all_files[i], lines=True)
        song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
        artist_data = list(df[['artist_id', 'artist_name', 'artist_location','artist_latitude', 'artist_longitude']].values[0])
        cur.execute(song_table_insert, song_data)
        cur.execute(artist_table_insert, artist_data)
        print('{}/{} files processed..'.format(i+1, len(all_files)))
        conn.commit()

#load data from a log_data to the time, user and songplay tbls
def process_log_file(cur, conn, filepath):
    all_files=[]
    for root, dirs, files in os.walk('data/log_data'):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    print('{} files are contained in "{}"'.format(len(all_files), filepath)) 

    for j in range(len(all_files)):
        df=pd.read_json(all_files[j], lines=True)
        #filter by page= NextSong
        df=df[df['page']=='NextSong']
        
        #convert timestamp column to datetime
        t=pd.to_datetime(df['ts'])
        
        #insert time data into records
        time_data=[(tt.value, tt.hour, tt.day, tt.week, tt.month, tt.year, tt.weekday()) for tt in t]
        col_labels=('timestamp', 'hour', 'day','week','month', 'year', 'weekday')
        time_df =pd.DataFrame(data=time_data, columns=col_labels)
        
        for i, row in time_df.iterrows():
            cur.execute(time_table_insert, list(row))

        #insert into user table
        user_df=df[['userId','firstName', 'lastName', 'gender', 'level']]
        for i, row in user_df.iterrows():
            cur.execute(user_table_insert, row)

        #insert into songplays table
        for index, row in df.iterrows():
            #print('{}: {}'.format(index, row))
            cur.execute(song_select, (row.song, row.artist, row.length))
            rst = cur.fetchone()
            if rst:
                songid, artistid = rst
            else:
                songid, artistid =None, None
            
            songplay_data = (index, row['ts'], row['userId'], row['level'], songid, artistid, row['sessionId'],row['location'], row['userAgent'])
            cur.execute(songplay_table_insert, songplay_data)
        conn.commit()
        print('{}/{} files processed..'.format(j+1, len(all_files)))    
        
            

def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=test password=test")
    cur = conn.cursor()

    #process_data(cur, conn, filepath='data/song_data')
    process_song_file(cur, conn, filepath='data/song_data')
    process_log_file(cur, conn, filepath='data/log_data')
    
            
            
            

            
            

    #print('worked')

    conn.close()
    

    
if __name__== '__main__':
    main()
