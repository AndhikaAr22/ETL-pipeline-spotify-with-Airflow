from dags.connector.koneksi import connection
from model.query import insert_data_log_path
from sqlalchemy import text
from airflow.models import Connection
from airflow.hooks.base import BaseHook
from google.cloud import bigquery

import json

postgres_conn = Connection.get_connection_from_secrets('neon_postgresql')
s3_conn = BaseHook.get_connection("spotify_minio")
extra = s3_conn.extra
extra_convt = json.loads(extra)

conn = connection(
    end_point=extra_convt['host'],
    aws_access_key_id=extra_convt['aws_access_key_id'],
    aws_secret_access_key=extra_convt['aws_secret_access_key'],
    postgres_host=postgres_conn.host,
    postgres_db=postgres_conn.schema,
    postgres_user=postgres_conn.login,
    postgres_password=postgres_conn.password,
    postgres_port=postgres_conn.port
)
# fuction load data log to db
def insert_log_data(path):
    path_component = path.split('/')
    file_name = path_component[5]
    s3_object = path  # Sebaiknya gunakan variabel file_name untuk mendapatkan nama objek
    

    aunt_postgres = conn.postgres_connection()
    connection = aunt_postgres.connect()  
    query = insert_data_log_path()
    connection.execute(text(query), {'file_name': file_name, 's3_object': s3_object})



    connection.close()
    print(f'name file = {file_name}')
    print(f'name S3 object = {s3_object}')

    print('Berhasil memasukkan data log ke tabel')
    return s3_object


class LoadData:
    client = bigquery.Client.from_service_account_json('/opt/airflow/dags/project-bq-satu-a2562e6d0487.json', project='project-bq-satu')
    def __init__(self, df_album, df_artist, df_song):
        self.df_album = df_album
        self.df_artist = df_artist
        self.df_song = df_song

    # load data album to bigquery
    def load_data_album(self):
        transform_album = self.df_album
        table_id = "project-bq-satu.spotify_data.album_data"

        schema = [
            bigquery.SchemaField("album_id", "STRING", "NULLABLE"),
            bigquery.SchemaField("album_name", "STRING", "NULLABLE"),
            bigquery.SchemaField("release_date", "STRING", "NULLABLE"),
            bigquery.SchemaField("total_track", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("album_type", "STRING", "NULLABLE"),
            bigquery.SchemaField("album_url", "STRING", "NULLABLE")
        ]
        try:
            job_config = bigquery.LoadJobConfig(
                schema=schema, 
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            job = self.client.load_table_from_dataframe(transform_album, table_id, job_config=job_config)
            # Menunggu hingga job selesai
            job.result()

            print("Data album telah dimuat ke BigQuery.")
        except Exception as e:
            print("Terjadi kesalahan saat memuat data ke BigQuery:", str(e))
        
    
    # load data album to bigquery
    def load_data_artist(self):
        transform_artist = self.df_artist
        table_id = "project-bq-satu.spotify_data.artist_data"

        schema = [
            bigquery.SchemaField("artist_id", "STRING", "NULLABLE"),
            bigquery.SchemaField("artist_name", "STRING", "NULLABLE"),
            bigquery.SchemaField("type", "STRING", "NULLABLE"),
            bigquery.SchemaField("artist_url", "STRING", "NULLABLE")
        ]
        try:
            job_config = bigquery.LoadJobConfig(
                schema=schema, 
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            job = self.client.load_table_from_dataframe(transform_artist, table_id, job_config=job_config)
            # Menunggu hingga job selesai
            job.result()

            print("Data artist telah dimuat ke BigQuery.")
        except Exception as e:
            print("Terjadi kesalahan saat memuat data ke BigQuery:", str(e))
        
    
    # load data album to bigquery
    def load_data_song(self):
        transform_song = self.df_song
        table_id = "project-bq-satu.spotify_data.song_data"


        schema = [
            bigquery.SchemaField("song_id", "STRING", "NULLABLE"),
            bigquery.SchemaField("song_name", "STRING", "NULLABLE"),
            bigquery.SchemaField("popularity", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("duration", "FLOAT", "NULLABLE"),
            bigquery.SchemaField("track_number", "INTEGER", "NULLABLE"),
            bigquery.SchemaField("song_url", "STRING", "NULLABLE")
        ]
        try:
            job_config = bigquery.LoadJobConfig(
                schema=schema, 
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
            job = self.client.load_table_from_dataframe(transform_song, table_id, job_config=job_config)
            # Menunggu hingga job selesai
            job.result()

            print("Data song telah dimuat ke BigQuery.")
        except Exception as e:
            print("Terjadi kesalahan saat memuat data ke BigQuery:", str(e))
        
        

# class LoadData:
#     client = bigquery.Client.from_service_account_json('/opt/airflow/dags/project-bq-satu-a2562e6d0487.json', project='project-bq-satu')
    
#     def __init__(self, df_album, df_artist, df_song):
#         self.df_album = df_album
#         self.df_artist = df_artist
#         self.df_song = df_song
#         postgres_conn = Connection.get_connection_from_secrets('neon_postgresql')
#         s3_conn = BaseHook.get_connection("spotify_minio")
#         extra = s3_conn.extra
#         extra_convt = json.loads(extra)
#         self.conn = connection(
#             end_point=extra_convt['host'],
#             aws_access_key_id=extra_convt['aws_access_key_id'],
#             aws_secret_access_key=extra_convt['aws_secret_access_key'],
#             postgres_host=postgres_conn.host,
#             postgres_db=postgres_conn.schema,
#             postgres_user=postgres_conn.login,
#             postgres_password=postgres_conn.password,
#             postgres_port=postgres_conn.port
#         )
#         self.client_minio = self.conn.minio_client()
#         self.postgres_conn = self.conn.postgres_connection()

#     # load data log
#     def insert_log_data(self, path):
#         path_component = path.split('/')
#         file_name = path_component[5]
#         s3_object = path  # Sebaiknya gunakan variabel file_name untuk mendapatkan nama objek
        

#         aunt_postgres = self.postgres_conn
#         connection = aunt_postgres.connect()  
#         query = insert_data_log_path()
#         connection.execute(text(query), {'file_name': file_name, 's3_object': s3_object})



#         connection.close()
#         print(f'name file = {file_name}')
#         print(f'name S3 object = {s3_object}')

#         print('Berhasil memasukkan data log ke tabel')
#         return s3_object

#     # load data album to bigquery
#     def load_data_album(self):
#         transform_album = self.df_album
#         table_id = "project-bq-satu.spotify_data.album_data"

#         schema = [
#             bigquery.SchemaField("album_id", "STRING", "NULLABLE"),
#             bigquery.SchemaField("album_name", "STRING", "NULLABLE"),
#             bigquery.SchemaField("release_date", "STRING", "NULLABLE"),
#             bigquery.SchemaField("total_track", "INTEGER", "NULLABLE"),
#             bigquery.SchemaField("album_type", "STRING", "NULLABLE"),
#             bigquery.SchemaField("album_url", "STRING", "NULLABLE")
#         ]
#         try:
#             job_config = bigquery.LoadJobConfig(schema=schema)
#             job = self.client.load_table_from_dataframe(transform_album, table_id, job_config=job_config)
#             # Menunggu hingga job selesai
#             job.result()

#             print("Data album telah dimuat ke BigQuery.")
#         except Exception as e:
#             print("Terjadi kesalahan saat memuat data ke BigQuery:", str(e))
        
    
#     # load data album to bigquery
#     def load_data_artist(self):
#         transform_artist = self.df_artist
#         table_id = "project-bq-satu.spotify_data.artist_data"

#         schema = [
#             bigquery.SchemaField("artist_id", "STRING", "NULLABLE"),
#             bigquery.SchemaField("artist_name", "STRING", "NULLABLE"),
#             bigquery.SchemaField("type", "STRING", "NULLABLE"),
#             bigquery.SchemaField("artist_url", "STRING", "NULLABLE")
#         ]
#         try:
#             job_config = bigquery.LoadJobConfig(schema=schema)
#             job = self.client.load_table_from_dataframe(transform_artist, table_id, job_config=job_config)
#             # Menunggu hingga job selesai
#             job.result()

#             print("Data artist telah dimuat ke BigQuery.")
#         except Exception as e:
#             print("Terjadi kesalahan saat memuat data ke BigQuery:", str(e))
        
    
#     # load data album to bigquery
#     def load_data_song(self):
#         transform_song = self.df_song
#         table_id = "project-bq-satu.spotify_data.song_data"


#         schema = [
#             bigquery.SchemaField("song_id", "STRING", "NULLABLE"),
#             bigquery.SchemaField("song_name", "STRING", "NULLABLE"),
#             bigquery.SchemaField("popularity", "INTEGER", "NULLABLE"),
#             bigquery.SchemaField("duration", "FLOAT", "NULLABLE"),
#             bigquery.SchemaField("track_number", "INTEGER", "NULLABLE"),
#             bigquery.SchemaField("song_url", "STRING", "NULLABLE")
#         ]
#         try:
#             job_config = bigquery.LoadJobConfig(schema=schema)
#             job = self.client.load_table_from_dataframe(transform_song, table_id, job_config=job_config)
#             # Menunggu hingga job selesai
#             job.result()

#             print("Data song telah dimuat ke BigQuery.")
#         except Exception as e:
#             print("Terjadi kesalahan saat memuat data ke BigQuery:", str(e))
        
        
