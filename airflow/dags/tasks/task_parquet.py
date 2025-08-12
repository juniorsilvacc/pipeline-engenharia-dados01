import pandas as pd
import boto3
import io
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def postgres_to_minio_etl_parquet(table_name: str, bucket_name: str, endpoint_url: str, access_key: str, secret_key: str):
    # Conectar ao cliente S3
    s3_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )
    
    # Etapa de leitura e escrita no S3
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=f"{table_name}/max_id.txt")
        max_id = int(obj['Body'].read().decode('utf-8'))
        logger.info(f"Max ID encontrado no S3: {max_id}")
    except s3_client.exceptions.NoSuchKey:
        max_id = 0
        logger.info("Nenhum max_id encontrado no S3, iniciando com max_id = 0")
    
    with PostgresHook(postgres_conn_id='postgres').get_conn() as pg_conn:
        with pg_conn.cursor() as pg_cursor:
            primary_key = f'id_{table_name}'

            pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
            columns = [row[0] for row in pg_cursor.fetchall()]
            columns_list_str = ', '.join(columns)

            pg_cursor.execute(f"SELECT {columns_list_str} FROM {table_name} WHERE {primary_key} > %s", (max_id,))
            rows = pg_cursor.fetchall()

            if rows:
                df = pd.DataFrame(rows, columns=columns)
                
                # Criar buffer para armazenar Parquet
                parquet_buffer = df.to_parquet(index=False)
                
                # Enviar para o MinIO
                s3_client.put_object(Bucket=bucket_name, Key=f"{table_name}/data_{max_id + 1}.parquet", Body=parquet_buffer)
                
                # Atualizar max_id
                max_id = df[primary_key].max()
                s3_client.put_object(Bucket=bucket_name, Key=f"{table_name}/max_id.txt", Body=str(max_id))