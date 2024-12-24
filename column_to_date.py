from pyspark.sql.functions import to_date

def column_to_date(df, column_name, format):
    """
    Converte uma coluna de string para o tipo de dado de data.

    Parâmetros:
    df (DataFrame): O DataFrame que contém a coluna a ser convertida.
    column_name (str): O nome da coluna que será convertida para data.
    format (str): O formato da data na coluna de string.

    Retorna:
    DataFrame: O DataFrame com a coluna convertida para o tipo de dado de data.
    """
    df = df.withColumn(column_name, to_date(column_name, format))
    return df