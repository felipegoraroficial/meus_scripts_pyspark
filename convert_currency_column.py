def convert_currency_column(df, col_name):
    """
    Converte uma coluna de moeda no DataFrame para o tipo double.

    Parâmetros:
    df (DataFrame): O DataFrame de entrada.
    col_name (str): O nome da coluna que contém os valores de moeda.

    Retorna:
    DataFrame: O DataFrame com a coluna de moeda convertida para double.
    """
    df = df.withColumn(col_name, regexp_replace(col(col_name), "R\\$\\s", ""))
    df = df.withColumn(col_name, regexp_replace(col(col_name), "\\.", ""))
    df = df.withColumn(col_name, regexp_replace(col(col_name), ",", "."))
    df = df.withColumn(col_name, col(col_name).cast("double"))

    return df