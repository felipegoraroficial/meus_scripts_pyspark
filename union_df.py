def union_df(df1, df2):
    """
    Une dois DataFrames pelo nome das colunas, permitindo colunas ausentes.

    Parâmetros:
    df1 (DataFrame): O primeiro DataFrame.
    df2 (DataFrame): O segundo DataFrame.

    Retorna:
    DataFrame: Um novo DataFrame resultante da união dos dois DataFrames de entrada.
    """
    return df1.unionByName(df2, allowMissingColumns=True)