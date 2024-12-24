def hidden_column(df):
    """
    Adiciona colunas extras ao DataFrame a partir da coluna '_rescued_data' e remove a coluna '_rescued_data'.
    Em seguida, extrai o valor da coluna 'DASH' usando uma expressão regular.

    Parâmetros:
    df (DataFrame): O DataFrame de entrada que contém a coluna '_rescued_data' com dados a serem processados.

    Retorna:
    DataFrame: O DataFrame modificado com as colunas extras adicionadas e a coluna '_rescued_data' removida.

    Detalhes:
    1. Define uma lista de nomes de colunas extras ('extra_columns') e seus tipos correspondentes ('extra_types').
    2. Divide a coluna '_rescued_data' em múltiplas colunas usando a função split() com vírgula como delimitador.
    3. Itera sobre as colunas extras e seus tipos, adicionando cada coluna ao DataFrame e convertendo para o tipo especificado.
    4. Remove a coluna '_rescued_data' do DataFrame.
    5. Usa uma expressão regular para extrair o valor da coluna 'DASH' e atualiza a coluna 'DASH' com o valor extraído.
    """
    extra_columns = ["DASH"]
    extra_types = ["string"]

    split_cols = split(col('_rescued_data'),",")

    for i, (col_name, col_type) in enumerate(zip(extra_columns, extra_types)):
        df = df.withColumn(col_name, split_cols.getItem(i).cast(col_type))

    df = df.drop('_rescued_data')

    df = df.withColumn("DASH",
                       regexp_extract(col("DASH"), r'"DASH":"(.*?)"', 1))
    
    return df