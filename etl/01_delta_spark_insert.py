from pyspark.sql.functions import mean, max, min, col, count
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("DeltaExercice")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DentalSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

#Importar o modulo das tabelas delta
from delta.tables import *

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


#Escreve a tabela em formato delta
print("Writing delta table...")
(
    enem
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("NU_ANO")
    .save("s3://datalake-igti-tf-tancredo-2021/staging-zone/enem")
)
