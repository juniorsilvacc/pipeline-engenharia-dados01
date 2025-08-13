import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import boto3
import io
import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def google_sheet_to_minio_etl(sheet_id, sheet_name, bucket_name, endpoint_url, access_key, secret_key):
    """
    Lê uma aba do Google Sheets, envia para o MinIO, grava no MariaDB
    e adiciona no dicionário df_dict para gerar CSV único depois.
    """
    
    # Configuração do cliente MinIO para upload dos dados em formato parquet
    minio_client = boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    def get_google_sheet_data(sheet_id, sheet_name):
        """
        Obtém dados de uma planilha do Google Sheets.
        """
        try:
            # Autenticação com Google Sheets via service account
            scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
            creds = Credentials.from_service_account_file('/opt/airflow/config_airflow/credentials.json', scopes=scope)
            client = gspread.authorize(creds)
            sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
            
            try:
                # Tenta ler todos os registros da planilha
                data = sheet.get_all_records()
            except gspread.exceptions.GSpreadException as e:
                # Tratar erro específico de cabeçalhos duplicados, fornecendo cabeçalhos esperados manualmente
                if 'A linha de cabeçalho na planilha não é única.' in str(e):
                    logging.warning(f"Erro ao usar get_all_records() (cabeçalhos duplicados): {e}")
                    expected_headers = {
                        'Clientes_Bike': ["ClienteID", "Cliente", "Estado", "Sexo", "Status"],
                        'Vendedores_Bike': ["VendedorID", "Vendedor"],
                        'Produtos_Bike': ["ProdutoID", "Produto", "Preco"],
                        'Vendas_Bike': ["VendasID", "VendedorID", "ClienteID", "Data", "Total"],
                        'ItensVendas_Bike': ["ProdutoID", "VendasID", "Quantidade", "ValorUnitario", "ValorTotal", "Desconto", "TotalComDesconto"]
                    }.get(sheet_name, None)
                    if expected_headers:
                        data = sheet.get_all_records(expected_headers=expected_headers)
                    else:
                        raise
            if not data:
                # Levanta erro se não houver dados
                raise ValueError(f"Nenhum dado foi retornado para a planilha {sheet_name}")

            df = pd.DataFrame(data)
            return df
        except Exception as e:
            logging.error(f"Erro ao obter dados da planilha do Google: {e}")
            raise

    try:
        # Obter dados e salvar no MinIO no formato parquet
        df = get_google_sheet_data(sheet_id, sheet_name)
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        minio_client.put_object(Bucket=bucket_name, Key=f"{sheet_name}/data.parquet", Body=parquet_buffer.getvalue())
    except Exception as e:
        logging.error(f"Erro ao processar a planilha {sheet_name}: {e}")
        raise
    
    # Conectar ao MariaDB e escrever os dados na tabela correspondente
    mysql_hook = MySqlHook(mysql_conn_id='mariadb_local')
    connection = mysql_hook.get_conn()

    try:
        with connection.cursor() as cursor:
            # Criar tabela se ainda não existir, usando todas as colunas como VARCHAR(255)
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {sheet_name} (
                {', '.join([f'{col} VARCHAR(255)' for col in df.columns])}
            )
            """
            cursor.execute(create_table_sql)
            logging.info(f"Tabela {sheet_name} verificada/criada no MariaDB")

            # Para cada linha, tenta atualizar registro existente ou inserir novo registro
            for index, row in df.iterrows():
                # Atualizar dados se já existir registro com a chave primária (primeira coluna)
                update_sql = f"""
                UPDATE {sheet_name}
                SET {', '.join([f'{col} = %s' for col in df.columns])}
                WHERE {df.columns[0]} = %s
                """
                cursor.execute(update_sql, tuple(row.tolist()) + (row[df.columns[0]],))

                # Inserir novo registro se não existir (INSERT IGNORE)
                insert_sql = f"""
                INSERT IGNORE INTO {sheet_name} ({', '.join(df.columns)})
                VALUES ({', '.join(['%s'] * len(df.columns))})
                """
                cursor.execute(insert_sql, tuple(row))

            connection.commit()
            logging.info(f"Dados inseridos/atualizados na tabela {sheet_name} no MariaDB")

    except Exception as e:
        logging.error(f"Erro ao conectar ao MariaDB ou inserir dados: {e}")

    finally:
        if connection:
            connection.close()
    logging.info("Processo ETL concluído com sucesso.")