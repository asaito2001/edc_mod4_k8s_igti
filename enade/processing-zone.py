#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
#import boto3

spark = (
    SparkSession.builder.appName("ExerciseSpark")
    .getOrCreate()
)


# Ler os dados do enade 2017
enade = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ";")
    .load("s3://datalake-desafio4-igti/data/landing-zone/MICRODADOS_ENADE_2017.txt")
)


enade.show()
enade.printSchema()

# Gravar no datalake como parquet
(
    enade
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3://datalake-desafio4-igti/data/processing-zone/enade2017")

)

spark.stop()