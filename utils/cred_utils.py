import configparser
from ruamel.yaml import YAML
from pyprojroot import here
import os
CRED_FILE = 'credentials'
def convert_credentials_to_yaml():
    # Read the credentials.txt file
    config = configparser.ConfigParser()
    input_credentials_file = os.path.join(
                            here(),"config", f"{CRED_FILE}.txt")
    config.read(input_credentials_file)
    
    # Get the profile name and credentials
    profile_name = config.sections()[0]
    aws_access_key_id = config.get(profile_name, 'aws_access_key_id')
    aws_secret_access_key = config.get(profile_name, 'aws_secret_access_key')
    aws_session_token = config.get(profile_name, 'aws_session_token')
    
    # Create a dictionary with the desired output format
    data = {
        'region_name': 'us-east-1',
        'profile_name': profile_name,
        'aws_access_key_id': aws_access_key_id,
        'aws_secret_access_key': aws_secret_access_key,
        'aws_session_token': aws_session_token
    }
    
    # Write the data to cred.yaml file in YAML format
    yaml = YAML()
    yaml.indent(mapping=2)
    credentials_yml_file =  os.path.join(
    here(),"config", f"{CRED_FILE}.yaml")
    with open(credentials_yml_file, 'w') as outfile:
        yaml.dump(data, outfile)

def read_yml_credentials(path):
    with open(path, 'r') as f:
        credentials = yaml.safe_load(f)
    return credentials

def anom_vals(dic):
    return {k:hashlib.sha256(str(v).encode()).hexdigest() for k,v in dic.items()}

convert_credentials_to_yaml()