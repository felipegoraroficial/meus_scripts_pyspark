def change_null_numeric(df, type):
    """
    Substitui valores nulos em colunas numéricas por 0.

    Parâmetros:
    df (DataFrame): O DataFrame de entrada.
    type (str): O tipo de dado das colunas numéricas (ex: 'int', 'double').

    Retorna:
    DataFrame: O DataFrame com valores nulos substituídos por 0 nas colunas numéricas especificadas.
    """
    numeric_columns = [col_name for col_name, data_type in df.dtypes if data_type == type]
    
    df = df.na.fill(0, subset=numeric_columns)

    return df