from pyspark.sql.functions import initcap, col

def text_string_columns(df):
    """
    Esta função recebe um DataFrame do Spark e capitaliza a primeira letra de cada palavra
    em todas as colunas de string.

    Parâmetros:
    df (DataFrame): O DataFrame do Spark que contém as colunas de string a serem modificadas.

    Retorna:
    DataFrame: Um novo DataFrame com as colunas de string modificadas.
    """
    string_column = [col_name for col_name, data_type in df.dtypes if data_type == 'string']

    for column in string_column:
        df = df.withColumn(column, initcap(col(column)))

    return df