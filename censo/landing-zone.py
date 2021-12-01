import zipfile
import requests
from io import BytesIO
import os
import boto3

# Cria um diretório para armazenar o conteúdo do enade 2017
os.makedirs('./censo2019', exist_ok=True)

print("Extracting data.....")

# Define a url e executa o download do conteúdo
url = "https://download.inep.gov.br/microdados/microdados_educacao_superior_2019.zip"
filebytes = BytesIO(requests.get(url).content)

print("Unzip files...")

# Extrai o conteúdo do zipfile
myzip = zipfile.ZipFile(filebytes)
myzip.extractall('./censo2019')

print("Upload to S3...")

# Realiza o Upload dos dados no S3 landing-zone/raw/
#s3_client = boto3.client("s3", aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
s3_client = boto3.client("s3")

s3_client.upload_file(
    "./censo2019/Microdados_Educaç╞o_Superior_2019/dados/SUP_CURSO_2019.CSV",
    "datalake-desafio4-igti",
    "data/landing-zone/censo2019/SUP_CURSO_2019.CSV"
)

s3_client.upload_file(
    "./censo2019/Microdados_Educaç╞o_Superior_2019/dados/SUP_ALUNO_2019.CSV",
    "datalake-desafio4-igti",
    "data/landing-zone/censo2019/SUP_ALUNO_2019.CSV"
)

s3_client.upload_file(
    "./censo2019/Microdados_Educaç╞o_Superior_2019/dados/SUP_DOCENTE_2019.CSV",
    "datalake-desafio4-igti",
    "data/landing-zone/censo2019/SUP_DOCENTE_2019.CSV"
)
