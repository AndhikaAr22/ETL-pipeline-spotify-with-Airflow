import json
from datetime import datetime
from dags.connector.koneksi import connection
from airflow.models import Connection
from airflow.hooks.base import BaseHook
from airflow.models import Variable

# class minio
class MinioBucket:
    def __init__(self):
        postgres_conn = Connection.get_connection_from_secrets('neon_postgresql')
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
        self.client_minio = self.conn.minio_client()
        self.postgres_conn = self.conn.postgres_connection()

    def upload_to_minio(self, data):
        crendetial = self.client_minio
        bucket_name = Variable.get("S3_BUCKET_NAME")

        try:
            crendetial.head_bucket(Bucket=bucket_name)
            print(f'Bucket {bucket_name} already exists.')
        except crendetial.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                crendetial.create_bucket(Bucket=bucket_name)
                print(f'Bucket {bucket_name} created successfully.')
            else:
                print(f'Error: {str(e)}')

        # create path for Minio object
        current_date = datetime.now()
        formatted_date = current_date.strftime("%Y%m%d")
        # path1 = f'{bucket_name}/project/{current_date.year}/{current_date.month}/{current_date.day}/spotify_data_{formatted_date}.json'
        path1 = f'{bucket_name}/project2/{current_date.year}/{current_date.month}/{current_date.day}/spotify_data_{formatted_date}.json'

        # Upload data to MinIO
        try:
            json_data = json.dumps(data)
            crendetial.put_object(Bucket=bucket_name, Key=path1, Body=json_data.encode('utf-8'))
            response = crendetial.list_objects(Bucket=bucket_name)
            for obj in response.get('Contents', []):
                print(obj['Key'])
            print(f'Successfully uploaded to MinIO: {path1}')
            return path1
        except Exception as e:
            print(f'Error uploading to MinIO: {str(e)}')
            return None
        
    def get_data_minio(self, object_param):
        # inisialisasi koneksi ke postgres
        connection = self.postgres_conn.connect() 
        # inisialisasi koneksi ke Minio
        crendetial = self.client_minio
        
        # melakukan query berdasarkan object_param ke tabel log
        query = f"select * from log1 where s3_object = '{object_param}' "
        result = connection.execute(query)
        data = result.fetchall()
        print(data)

        if len(data) == 0:
            print('Data tidak ada')
        else:
            bucket_name = Variable.get("S3_BUCKET_NAME")
            object_key = object_param

            try:
                response = crendetial.get_object(Bucket=bucket_name, Key=object_key)
                data_minio = response['Body'].read().decode('utf-8')
                data = json.loads(data_minio)
                return data

            except crendetial.exceptions.NoSuchKey as e:
                print(f'Objek dengan kunci {object_key} tidak ditemukan di bucket {bucket_name}')
            except Exception as e:
                print(f'Terjadi kesalahan saat mengambil objek: {str(e)}')
            finally:
                connection.close() 

    def json_parser(self, object_param):
        list_album = []
        list_song = []
        list_artist = []
        data_minio = self.get_data_minio(object_param)
        for i in data_minio:
            for item in i['tracks']:
                # album
                id_album = item['album']['id']
                name_album = item['album']['name']
                release_date = item['album']['release_date']
                total_track = item['album']['total_tracks']
                album_type = item['album']['album_type']
                url_album = item['album']['external_urls']['spotify']

                album = [id_album, name_album, release_date, total_track, album_type, url_album]
                list_album.append(album)
                # song
                id_song = item['id']
                name_song = item['name']
                popularity = item['popularity']
                duration = item['duration_ms']
                track_number = item['track_number']
                url_song = item['external_urls']['spotify']

                song = [id_song, name_song, popularity, duration, track_number, url_song]
                list_song.append(song)
                # artist
                id_artist = item['artists'][0]['id']
                name_artist = item['artists'][0]['name']
                type = item['artists'][0]['type']
                url_artist = item['artists'][0]['external_urls']['spotify']

                artist = [id_artist, name_artist, type, url_artist]
                list_artist.append(artist)

        return list_album, list_song, list_artist   