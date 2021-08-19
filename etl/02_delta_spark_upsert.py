import logging
import sys

from pyspark.sql.functions import mean, max, min, col, count, lit
from pyspark.sql import SparkSession

#Configuração de logs de aplicação
logging.basicConfig(stream=sys.stdout)
Logger = logging.getLogger('datalake_enem_small_upsert')
Logger.setLevel(logging.DEBUG)

#Definição da Spark Session
spark = (
    SparkSession.builder.appName("DeltaExercice")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DentalSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


Logger.info("Importing delta.tables...")

#Importar o modulo das tabelas delta
from delta.tables import *

logger.info("Produzindo novos dados...")
enemnovo = (
    spark.read.format("delta")
    .load("s3://datalake-igti-tf-tancredo-2021/staging-zone/enem")
)


#Define algumas inscrições (chaves) que serao alteradas
inscricoes = [190001595656,
            190001421546,
            190001133210,
            190001199383,
            190001237802
            ]

logger.info("Reduz a 5 casos e faz updates internos no município de residencia")
enemnovo = enemnovo.where(enemnovo.NU_INSCRICAO.isin(inscricoes))
enemnovo = enemnovo.withColumn("NO_MUNICIPIO_RESIDENCIA", lit("NOVA CIDADE").)withColumn("CO_MUNICIPIO_RESIDENCIA", lit(1000000))


logger.info("Pega os dados do Enem velhos na tabela Delta...")
enemvelho = DeltaTable.forPath(spark, "s3://datalake-igti-tf-tancredo-2021/staging-zone/enem")

logger.info("Realiza o UPSERT")
(
    enemvelho.alias("old")
    .merge(enemnovo.alias("new"), "old.NU_INSCRICAO = new.NU_INSCRICAO")
    .whenMatcheUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

logger.info("Atualização completa! \n\n")

logger.info("Gera manifesto symLink...")
enemvelho.generate("symlink_format_manifest")

logger.info("Manifesto gerado.")