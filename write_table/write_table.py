from pyspark.sql import SparkSession
from clize import run 
import os

def db(spark):
    return spark.read \
                .format("jdbc") \
                .option("driver", 'com.mysql.jdbc.Driver') \
                .option("url", "jdbc:mysql://mysql/tweeter?autoReconnect=true&useSSL=false") \
                .option("user", "root") \
                .option("password", "")

def write(table, outpath):
    spark = SparkSession \
        .builder \
        .appName("Dump MYSQL") \
        .config("spark.jars", "/home/jovyan/work/mysql-connector-java-5.1.48.jar,/home/jovyan/work/gcs-connector-hadoop2-latest.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/home/jovyan/work/key.json") \
	.config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    query = f'(SELECT min(id), max(id) FROM `{table}`) AS tmp'
    lower, upper = db(spark) \
        .option("dbtable", query) \
        .load() \
        .first()

    month = db(spark) \
        .option("dbtable", f"`{table}`") \
        .option("partitionColumn", "id") \
        .option("numPartitions","800") \
        .option("lowerBound",f"{lower}") \
        .option("upperBound",f"{upper}") \
        .load()

    month.write.mode('append').parquet(outpath)


if __name__ == '__main__':
    run(write)
