from pyspark.sql.functions import col, trim, regexp_replace
from pyspark.sql.types import StringType

def remove_extra_spaces(df):
    """
    Remove espaços em branco extras de todas as colunas de string em um DataFrame.

    Esta função percorre todas as colunas do tipo string no DataFrame fornecido
    e remove qualquer espaço em branco extra no início, fim e dentro das strings.

    Parâmetros:
    df (pyspark.sql.DataFrame): O DataFrame de entrada.

    Retorna:
    pyspark.sql.DataFrame: Um novo DataFrame com espaços extras removidos de todas as colunas de string.
    
    """
    string_cols = [col_name for col_name, col_type in df.dtypes if col_type == 'string']
    for col_name in string_cols:
        df = df.withColumn(col_name, regexp_replace(trim(col(col_name)), r'\s+', ' '))
    return df
