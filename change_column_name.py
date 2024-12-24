def change_column_name(df, col_original, col_change):
    """
    Altera o nome de uma coluna em um DataFrame.

    Parâmetros:
    df (DataFrame): O DataFrame no qual a coluna será renomeada.
    col_original (str): O nome original da coluna.
    col_change (str): O novo nome da coluna.

    Retorna:
    DataFrame: O DataFrame com a coluna renomeada.
    """
    df = df.withColumnRenamed(col_original, col_change)
    return df