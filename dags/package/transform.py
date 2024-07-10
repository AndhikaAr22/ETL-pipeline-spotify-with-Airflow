import pandas as pd
from connector.koneksi import connection
from airflow.models import Connection
from airflow.hooks.base import BaseHook
import json


# class untuk transform
class Transformer:
    def __init__(self, albums, songs, artists):
        self.albums = albums
        self.songs = songs
        self.artists = artists
        postgres_conn = Connection.get_connection_from_secrets('postgres_id')
        s3_conn = BaseHook.get_connection("spotify_minio")
        extra = s3_conn.extra
        extra_convt = json.loads(extra)
        self.conn = connection(
            end_point=extra_convt['host'],
            aws_access_key_id=extra_convt['aws_access_key_id'],
            aws_secret_access_key=extra_convt['aws_secret_access_key'],
            postgres_host=postgres_conn.host,
            postgres_db=postgres_conn.schema,
            postgres_user=postgres_conn.login,
            postgres_password=postgres_conn.password,
            postgres_port=postgres_conn.port

        )
        self.postgres_conn = self.conn.postgres_connection()

    
    def get_data_album(self):
        data_minio = self.albums
        
        column_album = ['album_id', 'album_name',  'release_date', 'total_track', 'album_type', 'album_url']

        df_album = pd.DataFrame(data_minio, columns= column_album)
        df_album = df_album.drop_duplicates(subset=['album_name'])
        engine = self.postgres_conn.connect()
        df_album.to_sql('album_table', con=engine, if_exists='replace', index=False)
        df_album.to_csv('/opt/airflow/dags/data/new_album1.csv', index=False)
        print('success get data album and ingest data to db')


        return df_album 
    
    # get data song
    def get_data_song(self):
        data_minio = self.songs
        column_song = ['song_id', 'song_name',  'popularity', 'duration', 'track_number', 'song_url']
        df_song = pd.DataFrame(data_minio, columns= column_song)
        df_song = df_song.drop_duplicates(subset=['song_name'])
        df_song['duration'] = df_song['duration'] / 60000
        engine = self.postgres_conn.connect()
        df_song.to_sql('song_table', con=engine, if_exists='replace', index=False)
        df_song.to_csv('/opt/airflow/dags/data/new_song.csv', index=False)
        print('success get data song and ingest data to db')

        return df_song
    
    # get data artist
    def get_data_artist(self):
        data_minio = self.artists
        column_artist = ['artist_id', 'artist_name',  'type',  'artist_url']
        df_artist = pd.DataFrame(data_minio, columns= column_artist)
        df_artist = df_artist.drop_duplicates(subset=['artist_name'])
        engine = self.postgres_conn.connect()
        df_artist.to_sql('artist_table', con=engine, if_exists='replace', index=False)
        df_artist.to_csv('/opt/airflow/dags/data/new_artist.csv', index=False)
        print('success get data artist and ingest data to db')

        return df_artist