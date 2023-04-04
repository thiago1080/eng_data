import os
import sys
from pyprojroot import here
sys.path.append(os.path.join(here(),"src","scripts"))
from get_data import read_yaml_file

credentials = os.path.join(
    here(), "slaitnederc.yaml"
)

ATHENA_PATH = "s3://ds-main-s3-200624937306-dataset"
CONFIG_PATH= os.path.join(
    here(), "config","globals.yaml"
)

x = read_yaml_file(CONFIG_PATH)
print(x)

