from pyspark.sql.functions import col,lower

def lower_string_column(df,column_name):
    """
    Converte todos os caracteres de uma coluna de string para minúsculas.

    Parâmetros:
    df (DataFrame): O DataFrame de entrada.
    column_name (str): O nome da coluna a ser convertida para minúsculas.

    Retorna:
    DataFrame: O DataFrame com a coluna especificada convertida para minúsculas.
    """
    df = df.withColumn(column_name, lower(col(column_name)))
    return df