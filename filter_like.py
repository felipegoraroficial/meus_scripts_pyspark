from pyspark.sql.functions import col

def filter_like(df, coluna, padrao):
    """
    Filtra os registros de um DataFrame onde os valores de uma coluna específica correspondem a um padrão regex.

    Args:
    df (DataFrame): O DataFrame a ser filtrado.
    coluna (str): O nome da coluna a ser filtrada.
    padrao (str): O padrão regex usado para filtrar os valores da coluna.

    Returns:
    DataFrame: Um novo DataFrame contendo apenas os registros que correspondem ao padrão regex.
    """
    return df.filter(col(coluna).rlike(padrao))


