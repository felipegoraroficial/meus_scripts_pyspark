from pyspark.sql.functions import col,upper

def upper_string_column(df,column_name):
    """
    Converte todos os caracteres de uma coluna de string para maiúsculas.

    Parâmetros:
    df (DataFrame): O DataFrame de entrada.
    column_name (str): O nome da coluna que será convertida para maiúsculas.

    Retorna:
    DataFrame: O DataFrame com a coluna especificada convertida para maiúsculas.
    """
    df = df.withColumn(column_name, upper(col(column_name)))

    return df