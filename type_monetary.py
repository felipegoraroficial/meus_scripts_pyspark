from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, regexp_extract, lit

def type_monetary(df: DataFrame, column: str) -> DataFrame:
    """
    Atualiza a coluna 'moeda' no DataFrame com base em condições específicas.

    :param df: DataFrame do PySpark.
    :param column: Nome da coluna a ser analisada para identificação da moeda.
    :return: DataFrame atualizado.
    """
    df = df.withColumn(
        "moeda",
        when(
            col(column).contains("R$"),
            lit("R$")
        ).when(
            col(column).rlike(r"[$€£¥]"),
            regexp_extract(col(column), r"([$€£¥])", 1)
        ).otherwise(lit("moeda não identificada"))
    )
    return df

