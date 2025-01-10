from pyspark.sql.functions import col, regexp_extract

def extract_characters(df, col_name, col_extract, padrao):
    """
    Extrai caracteres específicos de uma coluna e coloca o resultado em outra coluna do DataFrame.

    :param df: DataFrame do PySpark.
    :param col_name: Nome da coluna onde o resultado da extração será armazenado.
    :param col_extract: Nome da coluna da qual os caracteres serão extraídos.
    :param padrao: O padrão de regex usado para extrair os caracteres.
    :return: DataFrame atualizado.
    """
    df = df.withColumn(col_name, regexp_extract(col(col_extract), padrao, 1))
    return df
