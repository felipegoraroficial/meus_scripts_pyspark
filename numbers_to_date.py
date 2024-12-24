def numbers_to_date(df, coluna):
    """
    Converte uma coluna de números em datas.

    Parâmetros:
    df (DataFrame): O DataFrame de entrada.
    coluna (str): O nome da coluna que contém os números a serem convertidos.

    Retorna:
    DataFrame: O DataFrame com a coluna convertida para datas.
    """
    df = df.withColumn(coluna, col(coluna).cast(IntegerType()))
    df = df.withColumn(coluna, to_date(expr(f"date_add('1899-12-30', `{coluna}`)")))
    return df