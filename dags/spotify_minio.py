import boto3
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

# task untuk mengambil data spotify dan load data ke minio
def task_1():
    from package.package_spotify import Spotify
    from package.package_minio import MinioBucket
    url_param = Variable.get('url_search_spotify')
    spotify_instance = Spotify(url_param)
    data = spotify_instance.get_all_data_spotify()
    minio_instance = MinioBucket()
    insert_data_to_minio = minio_instance.upload_to_minio(data)

    return insert_data_to_minio

# task untuk mengambil data dari minio by data log
def task_2(**kwargs):
    from package.load_data import insert_log_data
    task_instance = kwargs['task_instance']
    path = task_instance.xcom_pull(task_ids='get_data_insert_to_minio')
    if path is None:
        raise ValueError("Path is None")
    s3_object = insert_log_data(path)
    return s3_object
    
# task untuk transform data dan load data to DB
def task_3(**kwargs):
    from package.transform import Transformer
    from package.package_minio import MinioBucket
    task_instance = kwargs['task_instance']
    s3_object = task_instance.xcom_pull(task_ids='cek_path_minio')
    minio_instance = MinioBucket()
    list_album, list_song, list_artist = minio_instance.json_parser(s3_object)
    # transformer
    transformer = Transformer(albums=list_album, songs=list_song, artists=list_artist)
    transformer.get_data_album()
    transformer.get_data_artist()
    transformer.get_data_song()



default_args = {
    'owner': 'Andhika',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': timedelta(days=1),
    
}

# Definisikan DAG
with DAG(
    dag_id='dag_project_spotify_minio',
    start_date=datetime(2023, 6, 23),
    description='dag ini bertujuan untuk mengambil data dari spotify api',
    schedule_interval=None,  
    catchup=False
) as dag:
    start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
    )
    task_satu = PythonOperator(
        task_id = 'get_data_insert_to_minio',
        python_callable = task_1
    )
    task_dua = PythonOperator(
        task_id = 'cek_path_minio',
        python_callable = task_2
    )
    task_tiga = PythonOperator(
        task_id = 'load_data_to_db',
        python_callable = task_3
    )

    end_task = DummyOperator(
        task_id='end_task',
        dag=dag,
    )



    start_task >> task_satu 
    task_satu >> task_dua
    task_dua >> task_tiga
    task_tiga >> end_task
