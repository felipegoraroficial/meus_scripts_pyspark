from pyspark.sql.functions import explode, col, regexp_extract, to_date, row_number, input_file_name
from pyspark.sql.window import Window

def process_data_to_bronze(df, column_id):
    """
    Processa os dados para a camada bronze.

    Parâmetros:
    df (DataFrame): DataFrame de entrada contendo os dados a serem processados.
    column_id (str): Nome da coluna usada para particionar os dados.

    Retorna:
    DataFrame: DataFrame processado com os dados mais recentes por coluna_id.
    """
    
    df = df.withColumn("file_name", input_file_name())
    df = df.withColumn("file_date", regexp_extract(col("file_name"), r'\d{4}-\d{2}', 0))
    df = df.withColumn("file_date", to_date(col("file_date"), "yyyy-MM"))

    window_spec = Window.partitionBy(column_id).orderBy(col("file_date").desc())

    df = df.withColumn("row_number", row_number().over(window_spec))

    df = df.filter(col("row_number") == 1).drop("row_number")

    return df