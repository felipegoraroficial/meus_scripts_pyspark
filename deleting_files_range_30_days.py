from databricks.sdk.runtime import *
from datetime import datetime, timedelta

def deleting_files_range_30(file_path):
    """
    Deleta arquivos em um diretório especificado que são mais antigos que 30 dias.

    Parâmetros:
    file_path (str): O caminho do diretório onde os arquivos serão verificados e possivelmente deletados.

    A função lista todos os arquivos no diretório especificado e verifica a idade de cada arquivo.
    Se a idade do arquivo for maior que 30 dias, o arquivo será deletado.
    """
    blobs = dbutils.fs.ls(file_path)
    current_time = datetime.now()
    age_limit = timedelta(days=30)

    for blob in blobs:
        file_info = dbutils.fs.ls(blob.path)
        for info in file_info:
            file_age = current_time - datetime.fromtimestamp(info.modificationTime / 1000)
            if file_age > age_limit:
                dbutils.fs.rm(info.path)
                print(f"Deletando {info.name} porque é mais velho que 30 dias.")
            else:
                print(f"arquivo {info.name} com idade de {file_age.days} dias, não está dentro do limite de 30 dias.")