def numbers_to_date(df,coluna):

    df = df.withColumn(coluna,col(coluna).cast(IntegerTyoe()))

    df = df.withColumn(coluna,to_date(expr(f"date_add('1899-12-30',`{coluna}`)")))

    return df