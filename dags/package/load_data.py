from dags.connector.koneksi import connection
from model.query import insert_data_log_path
from sqlalchemy import text
from airflow.models import Connection
from airflow.hooks.base import BaseHook

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
# fungsi untuk load data
def insert_log_data(path):
    path_component = path.split('/')
    file_name = path_component[5]
    s3_object = path  # Sebaiknya gunakan variabel file_name untuk mendapatkan nama objek
    

    aunt_postgres = conn.postgres_connection()
    connection = aunt_postgres.connect()  # Use the connect method on the Engine
    # query = text(insert_data_log_path())
    query = insert_data_log_path()
    # connection.execute(query, (file_name, s3_object))
    # connection.execute(query, [(file_name, s3_object)])
    connection.execute(text(query), {'file_name': file_name, 's3_object': s3_object})
    # connection.commit()


    connection.close()
    print(f'name file = {file_name}')
    print(f'name S3 object = {s3_object}')

    print('Berhasil memasukkan data log ke tabel')
    return s3_object
