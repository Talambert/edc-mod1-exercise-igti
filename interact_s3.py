import boto3
import pandas as pd

#Criar um cliente para interagir com o QWS S3
s3_client = boto3.client('s3')

s3_client.download_file("datalake-tancredo-igti-edc",
                        "data/cidades.csv",
                        "vendas.csv")

df = pd.read_csv("vendas.csv", sep=";")
print(df)

s3_client.upload_file("pessoas.csv",
                    "datalake-tancredo-igti-edc",
                    "data/pessoas.csv"
                    )

df = pd.read_csv("pessoas.csv", sep=";")
print(df)