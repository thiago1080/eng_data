import boto3


# Create an Athena client
athena = session.client('athena')


# Set up the query execution parameters
database = 'your_database_name'
output_location = 's3://your-bucket-name/prefix/'

# Execute the query
response = athena.start_query_execution(
    QueryString=query,
    QueryExecutionContext={'Database': database},
    ResultConfiguration={'OutputLocation': output_location}
)

# Get the query execution ID
query_execution_id = response['QueryExecutionId']

# Retrieve the results of the query execution
results_paginator = athena.get_paginator('get_query_results')
results_iter = results_paginator.paginate(
    QueryExecutionId=query_execution_id,
    PaginationConfig={
        'PageSize': 1000,
        'StartingToken': None
    }
)