from pyspark.sql.functions import mean, max, min, col, count
from pyspark.sql import SparkSession

spark = (
SparkSession.builder.appName("ExerciseSpark")
    .getOrCreate()
)


# Ler os dados no ENEM 2019
enem = ( 
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ";")
    .load("s3://datalake-gloria-556116348126/raw-data/enem/")

)

# Criando um parquet. Atenção: Esse command não será executado neste notebook!

(
   enem
    .write
   .mode("overwrite")
   .format("parquet")
   .partitioning("year")
   .save("s3://datalake-gloria-556116348126/staging/enem")
)
