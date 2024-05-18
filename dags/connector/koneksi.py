import boto3
from sqlalchemy import create_engine


# class untuk koneksi 
class connection:
    def __init__(self, end_point, aws_access_key_id, aws_secret_access_key, postgres_host, postgres_db, postgres_user, 
                 postgres_password, postgres_port) :
        self.end_point = end_point
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.postgres_host = postgres_host
        self.postgres_db = postgres_db
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_port = postgres_port

    def minio_client(self):
        s3_client = boto3.client(
            's3',
            endpoint_url=self.end_point,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key
        )
        return s3_client
    
    def postgres_connection(self):
        try:
            # membuat koneksi database postgreSQL
            conn_str = f'postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}'
            engine = create_engine(conn_str)
            print("koneksi ke postgreSQL berhasil!")
        except Exception as error:
            print("gagal koneksi ke postgreSQL:", error)
            engine = None
        return engine

