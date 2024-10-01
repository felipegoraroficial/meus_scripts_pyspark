def hidden_column(df):

    extra_columns = ["DASH"]
    extra_types = ["string"]

    split_cols = split(col('_rescued_data'),",")

    for i, (col_name,col_type) in enumerate(zip(extra_columns,extra_types)):
        df = df.withColumn(col_name,split_cols.getItem(i).cast(col_type))

    df = df.drop('_rescued_data')

    df = df.withColumn("DASH",
                       regexp_extract(col("DASH"), r'"DASH":"(.*?)"',1))
    
    return df