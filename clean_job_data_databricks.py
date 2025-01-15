from pyspark.sql.functions import from_unixtime, col, udf, regexp_extract, round, when, split, to_date, date_format
from pyspark.sql.types import StringType

def clean_job_data_databricks(df):
    """
    Limpa e transforma os dados de execução de jobs no Databricks.

    Parâmetros:
    df (DataFrame): DataFrame contendo os dados brutos dos jobs.

    Retorna:
    DataFrame: DataFrame com os dados limpos e transformados.
    """

    # Converte 'start_time' e 'end_time' de milissegundos para timestamp
    df = df.withColumn('start_time', (col('start_time') / 1000).cast('timestamp'))
    df = df.withColumn('end_time', (col('end_time') / 1000).cast('timestamp'))

    # Função para converter duração de milissegundos para formato 'Xm:YS'
    def convert_durantion(ms):
        seconds = ms // 1000
        minutes = seconds // 60
        remaining_seconds = seconds % 60
        return f'{minutes}m:{remaining_seconds}s'

    # Cria UDF para converter duração
    convert_durantion_udf = udf(convert_durantion, StringType())

    # Aplica UDF para converter a coluna 'run_duration'
    df = df.withColumn('run_duration', convert_durantion_udf(col('run_duration')))

    # Extrai e renomeia colunas de detalhes de terminação
    df = df.withColumn('state', col('status.termination_details.code')) \
        .withColumn('message', col('status.termination_details.message')) \
        .drop('status')
    
    # Substitui valores de 'state' para 'FAILED' onde aplicável
    df = df.withColumn('state', when(col('state') == 'RUN_EXECUTION_ERROR', 'FAILED').otherwise(col('state')))

    # Substitui valores de 'state' para 'SUCCESS' onde for nulo
    df = df.withColumn('state', when(col('state').isNull(), 'SUCCESS').otherwise(col('state')))

    # Substitui valores de 'message' para 'workflow running' onde 'state' for nulo
    df = df.withColumn('message', when(col('state') == 'SUCCESS', 'workflow running').otherwise(col('message')))

    # Separa 'start_time' em 'start_date' e 'start_time'
    df = df.withColumn('start_date', to_date(split(col('start_time'), ' ')[0], 'yyyy-MM-dd')) \
        .withColumn('start_time', date_format(split(col('start_time'), ' ')[1], 'HH:mm:ss.SSS'))

    # Separa 'end_time' em 'end_date' e 'end_time'
    df = df.withColumn('end_date', to_date(split(col('end_time'), ' ')[0], 'yyyy-MM-dd')) \
        .withColumn('end_time', date_format(split(col('end_time'), ' ')[1], 'HH:mm:ss.SSS'))

    # Seleciona colunas específicas para o DataFrame final
    df = df.select('run_name', 'run_id', 'creator_user_name', 'start_date', 'start_time', 'end_date', 'end_time', 'run_duration', 'trigger', 'run_page_url', 'state', 'message')

    # Extrai minutos e segundos de 'run_duration' e calcula a duração total em minutos
    df = df.withColumn('run_duration_minutes', regexp_extract(col('run_duration'), '(\d+)m', 1).cast('double')) \
        .withColumn('run_duration_seconds', regexp_extract(col('run_duration'), '(\d+)s', 1).cast('double')) \
        .withColumn('run_duration', round(col('run_duration_minutes') + col('run_duration_seconds') / 60, 2)) \
        .drop('run_duration_minutes', 'run_duration_seconds')

    # Ordena o DataFrame por 'start_date' e 'start_time' em ordem decrescente
    df = df.orderBy(col('start_date').desc(), col('start_time').desc())

    return df