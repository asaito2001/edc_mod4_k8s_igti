from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

# set conf
conf = (
SparkConf()
    .set("spark.hadoop.fs.s3a.fast.upload", True)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
    .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3")
)

# apply config
sc = SparkContext(conf=conf).getOrCreate()

if __name__ == "__main__":

    # init spark session
    spark = SparkSession\
            .builder\
            .appName("Censo Job Docente")\
            .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = (
        spark
        .read
        .format("csv")
        .options(header='true', inferSchema='true',delimiter='|')
        .load("s3a://datalake-desafio4-igti/data/landing-zone/censo2019/SUP_DOCENTE_2019.CSV")
    )

    df.printSchema()

    (df
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3a://datalake-desafio4-igti/data/processing-zone/censo2019/sup_docente")
    )

    print("**********************")
    print(" Escrito com sucesso! ")
    print("**********************")

    spark.stop()