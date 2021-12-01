#from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
#import boto3

spark = (
    SparkSession.builder.appName("ExerciseSpark")
    .getOrCreate()
)


# Ler os dados do censo 2019
censo = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", "|")
    .load("s3://datalake-desafio4-igti/data/landing-zone/censo2019/SUP_ALUNO_2019.CSV")
)


censo.show()
censo.printSchema()

# Gravar no datalake como parquet
(
    censo
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3://datalake-desafio4-igti/data/processing-zone/censo2019/sup_aluno")

)

spark.stop()