def convert_currency_column(df,col_name):

    df = df.withColumn(col_name, regexp_replace(col(col_name), "R\\$\\s",""))
    df = df.withColumn(col_name, regexp_replace(col(col_name), "\\.",""))
    df = df.withColumn(col_name, regexp_replace(col(col_name), ",","."))
    df = df.withColumn(col_name, col(col_name).cast("double"))

    return df
                       
