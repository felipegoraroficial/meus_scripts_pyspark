def change_null_string(df):
    """
    Substitui valores nulos em colunas de string por '-'.

    Parâmetros:
    df (DataFrame): O DataFrame de entrada.

    Retorna:
    DataFrame: O DataFrame com valores nulos substituídos por '-'.
    """
    string_columns = [col_name for col_name, data_type in df.dtypes if data_type == 'string']
    df = df.na.fill('-', subset = string_columns)

    return df