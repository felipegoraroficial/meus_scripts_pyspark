from pyspark.sql.functions import col, when

def condition_like(df, new_column_name, condition_column, pattern):
    """
    Adiciona uma nova coluna ao DataFrame com valores 'Sim' ou 'Nao' 
    com base em uma condição de correspondência de padrão.

    Parâmetros:
    df (DataFrame): O DataFrame de entrada.
    new_column_name (str): O nome da nova coluna a ser adicionada.
    condition_column (str): O nome da coluna existente a ser verificada.
    pattern (str): O padrão regex a ser correspondido na coluna condition_column.

    Retorna:
    DataFrame: O DataFrame com a nova coluna adicionada.
    """
    df = df.withColumn(new_column_name, when(col(condition_column).rlike(pattern), 'Sim').otherwise('Nao'))
    return df