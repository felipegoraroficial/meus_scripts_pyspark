def read_file(path, file,sep):

    input_file = path + file

    df = spark.read.format(file) \
    .option("sep",sep) \
    .option("quote",'"') \
    .option("escape",'"') \
    .option("multiLine","true") \
    .option("header","true") \
    .option("rescuedDataColumn","_rescued_data") \
    .option("inferSchema","true") \
    .load(input_file)

    return df

