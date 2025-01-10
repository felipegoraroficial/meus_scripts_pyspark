from pyspark.sql.functions import regexp_replace

def replace_characters(df, coluna, caracter, substituto):
    """
    Substitui um caracter específico por outro em uma coluna do DataFrame.

    :param df: DataFrame do PySpark.
    :param coluna: Nome da coluna onde o caracter deve ser substituído.
    :param caracter: O caracter a ser substituído.
    :param substituto: O caracter substituto.
    :return: DataFrame atualizado.
    """
    df = df.withColumn(coluna, regexp_replace(coluna, caracter, substituto))
    return df

