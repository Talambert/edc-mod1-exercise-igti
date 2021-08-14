from pyspark.sql.functions import mean, max, min, col, count
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("ExerciceSpark")
    .getOrCreate()
)

#leitura
enem = (
    spark
    .read
    .format("CSV")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ";")
    .load("s3://datalake-tancredo-421168935276/raw-data/")
)


#Transformação em parquet
(
 enem
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("NU_ANO")
    .save("s3://datalake-tancredo-421168935276/consumer-zone/")
)
