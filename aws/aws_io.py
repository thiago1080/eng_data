import boto3
import pandas as pd
import yaml
import os
from pyprojroot import here
from loguru import logger


CREDENTIALS_FILE_PATH = os.path.join(
    here(),"config", "slaitnederc.yaml"
)


s3 = boto3.resource('s3')
bucket = s3.Bucket('//ds-main-s3-200624937306-dataset')
file_name = '/ds-sup-vb_psico_antib/topsis/planilhas_base/planilhao_vals.parquet'


# for obj in bucket.objects.all():
#     key = obj.key
#     body = obj.get()['Body'].read()
    
with open(CREDENTIALS_FILE_PATH, 'r') as f:
    credentials = yaml.safe_load(f)


profile_name=credentials['profile_name']
aws_access_key_id=credentials['aws_access_key_id']
aws_secret_access_key=credentials['aws_secret_access_key']
region_name=credentials['region_name']
aws_session_token = credentials['aws_session_token']

bucket_name = 'ds-main-s3-200624937306-dataset'
bucket = s3.Bucket(bucket_name)
file_name = 'ds-sup-vb_psico_antib/topsis/planilhas_base/vendas_colunas_valores.xlsx'
file_name = 'ds-sup-vb_psico_antib/topsis/planilhas_base/planilhao_vals.parquet'
sql_test_path = os.path.join(here(),'sql_queries','dummy_query.sql')


def read_sql_query(filepath: str) -> str | None:
    with open(filepath, "r") as stream:
        try:
            with open(filepath, 'r') as file:
                query = file.read()
                logger.info(f"Leitura com sucesso do Arquivo .sql")
            return query
        except yaml.YAMLError as exc:
            logger.error(exc)

def format_string(string, **kwargs):
    #todev
    try:
        return string.format(**kwargs)
    except Exception as e:
        print('Error: {}'.format(e))

    
# format_string(read_sql_query(sql_test_path ),cd_produto=28)

def load_dataframe_from_s3(bucket_name, file_key, aws_access_key_id, aws_secret_access_key):
    """
    Load a Pandas DataFrame from an S3 bucket and return it.
    """
    # create S3 client with credentials
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token = aws_session_token)
    
    # load data from S3 bucket
    print(type(bucket_name))
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read()
    df = pd.read_parquet(data)
    logger.success(f'Sucesso. Dataframe contém {df.shape[0]} linhas')
    return df

#load_dataframe_from_s3(bucket_name, file_name, aws_access_key_id, aws_secret_access_key)


def parse_s3(bucket_name: str, file_name: str, s3_prefix = 's3', site_name = 's3-website-ap-southeast-2.amazonaws.com'):
    return f'{s3_prefix}:/{bucket_name}.{site_name}/{file_name}'

def old_load_dataframe_from_s3(bucket_name, file_key):
    """
    Load a Pandas DataFrame from an S3 bucket and return it.
    """
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    return  pd.read_parquet(data)

