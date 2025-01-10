def verify_new_lines(df1, df2, condicao_join):
    """
    Verifica novas linhas em df1 que não estão presentes em df2 com base na condição de junção fornecida.

    Parâmetros:
    df1 (DataFrame): O primeiro DataFrame a ser comparado.
    df2 (DataFrame): O segundo DataFrame a ser comparado.
    condicao_join (str ou lista): A condição de junção para comparar os DataFrames.

    Retorna:
    DataFrame: Um DataFrame contendo as linhas de df1 que não estão presentes em df2.
    """
    df = df1.join(df2, condicao_join, how="left_anti")

    return df