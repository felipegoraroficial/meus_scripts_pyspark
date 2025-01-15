from pyspark.sql.functions import max

def filter_by_max_date(df, column_date):
    """
    Filtra o DataFrame para manter apenas as linhas com a data máxima.

    Parâmetros:
    df (DataFrame): O DataFrame de entrada.
    column_date (str): O nome da coluna que contém as datas.

    Retorna:
    DataFrame: Um DataFrame filtrado contendo apenas as linhas com a data máxima.
    """
    max_date = df.select(max(column_date)).collect()[0][0]
    df = df.filter(df[column_date] == max_date)

    return df