import boto3
from loguru import logger
import os
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
from pyprojroot import here
# set up logging
import yaml





CREDENTIALS_FILE_PATH = os.path.join(
    here(), "slaitnederc.yaml"
)


with open(CREDENTIALS_FILE_PATH, 'r') as f:
    credentials = yaml.safe_load(f)

session = boto3.Session(**credentials)
athena_client = session.client('athena')

# specify the S3 staging directory for Athena
athena_path = 's3://ds-main-s3-200624937306-dataset/ds-sup-vb_psico_antib/topsis/default/'

# create the Athena cursor
athena_cursor = connect(
    s3_staging_dir=athena_path,
    cursor_class=PandasCursor,
    **credentials,
).cursor()




# execute the query
query = 'SELECT * FROM RD_EXT_A_RAIABD.TB_NF_ITEM LIMIT 10'
athena_cursor.execute(query)

# fetch the results as a Pandas dataframe
df = athena_cursor.fetchall()

# log the number of rows returned
logger.success(f"Query returned {len(df)} rows")

# do something with the dataframe
