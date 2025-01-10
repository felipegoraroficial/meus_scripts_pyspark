import re
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

def extract_memory(df, column_name):
    """
    Adiciona uma coluna com a quantidade de memória em GB extraída de outra coluna do DataFrame.

    Parâmetros:
    df (DataFrame): O DataFrame original.
    column_name (str): O nome da coluna que contém a informação de memória.

    Retorna:
    DataFrame: O DataFrame com a nova coluna 'memoria'.
    """
    
    def extract_memory_info(info):
        """
        Extrai a quantidade de memória em GB de uma string fornecida.

        Parâmetros:
        info (str): A string contendo a informação de memória.

        Retorna:
        str: A quantidade de memória em GB encontrada na string ou '-' se não encontrada.
        """
        if isinstance(info, str) and info:
            padrao = r'(\d+)\s*(G[Bb])'
            resultado = re.search(padrao, info, re.IGNORECASE)
            if resultado:
                return resultado.group(0)
        return '-'

    extrair_memoria_udf = udf(extract_memory_info, StringType())
    return df.withColumn('memoria', extrair_memoria_udf(col(column_name)))