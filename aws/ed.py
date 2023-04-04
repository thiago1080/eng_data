import boto3
import re
from tqdm import tqdm
from collections import defaultdict

def describe_table(result):
    tabela={}
    for i in result['ResultSet']['Rows']:
        values = i['Data'][0]['VarCharValue'].split()
        if len(values)>1: 
            tabela[values[0]] = values[1]
    return tabela

def get_table_cols(result):
    cols=[]
    for i in result['ResultSet']['Rows']:
        values = i['Data'][0]['VarCharValue'].split()
        if len(values)>1: 
            cols.append(values[0])
    return cols

def get_cv_cols(df):
    cols_raw = [re.findall('\[(.*?)\]', i) for i in df.columns.values]
    cols=[]
    for i in cols_raw:
        cols.append(i[0].lower())
    return cols
def get_databases(client = boto3.client('athena'),CatalogName = 'AwsDataCatalog'):
    response_lsdb = client.list_databases(
        CatalogName = CatalogName
    )
    databases = [i['Name'] for i in response_lsdb['DatabaseList']]
    return databases
def get_tables(client = boto3.client('athena'), CatalogName = 'AwsDataCatalog', DatabaseName = None):
    response_tables = client.list_table_metadata(
        CatalogName = CatalogName,
        DatabaseName = DatabaseName
    )
    tables = [i['Name'] for i in response_tables['TableMetadataList']]
    return tables
def get_at_columns(glue_client = boto3.client('glue'), DatabaseName = None, Name = None):

    response_cols = glue_client.get_table(
        DatabaseName = DatabaseName,
        Name = Name
    )
    cols = [i['Name'] for i in response_cols['Table']['StorageDescriptor']['Columns']]
    return cols

def isin(word, string):
    if word.lower() in string.lower():
        return True
    else:
        return False

def search_col(db,list_cols):
    list_cols = ['cupom']
    db_found = defaultdict(dict)
    for db in tqdm(get_databases()):    
        for table in get_tables(DatabaseName = db):
            n_found_cols = 0
            found_cols = []
            for col_db in get_at_columns(DatabaseName=db, Name = table):           
                for wanted_col in list_cols:
                    if isin(wanted_col, col_db):
                        found_cols.append(col_db)
                        n_found_cols+=1
            if n_found_cols>0:
                db_found[db][table] = (n_found_cols, found_cols)
                        #print(db, table, n_found_cols)
    return db_found


def search_table(db,list_tables):
    list_tables= ['cupom']
    db_found = defaultdict(dict)
    for db in tqdm(get_databases()):    
        n_found_tables = 0
        found_tables = []
        for table in get_tables(DatabaseName = db):
            for wanted_table in list_tables:
                if isin(wanted_table, table):
                    found_tables.append(table)
                    n_found_tables+=1
        if n_found_tables>0:
            db_found[db] = (n_found_tables, found_tables)
    return db_found