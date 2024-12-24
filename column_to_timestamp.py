from pyspark.sql.functions import to_timestamp

def column_to_timestamp(df,column_name,format):
    """
    Converte uma coluna de um DataFrame para o tipo timestamp.

    Parâmetros:
    df (DataFrame): O DataFrame de entrada.
    column_name (str): O nome da coluna a ser convertida.
    format (str): O formato da data/hora da coluna.

    Retorna:
    DataFrame: O DataFrame com a coluna convertida para timestamp.
    """
    df = df.withColumn(column_name, to_timestamp(column_name,format))

    return df