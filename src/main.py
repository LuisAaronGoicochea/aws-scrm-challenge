import os
import json
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from data_processor import DataProcessor
from get_keys import get_secret


def main():

    # Obtiene las variables de acceso desde Secrets Manager
    secret = get_secret()
    secret_data = json.loads(secret[0])
    access_key = secret_data["access_key"]
    secret_access_key = secret_data["secret_key"]
    
    # Creando la configuración de Spark
    conf = SparkConf().setAppName("scrm-challenge")
    sc = SparkContext(conf=conf)

    # Creando la sesión de Spark
    spark = SparkSession(sc).builder.appName("scrm-challenge-app") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_access_key) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", access_key)
    hadoopConf.set("fs.s3a.secret.key", secret_access_key)
    hadoopConf.set("spark.hadoop.fs.s3a.aws.credential.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    data_processor = DataProcessor(spark)
    raw_path = "s3a://scrm-challenge-raw/scrm/raw/data"
    data_paths = [raw_path + "/products.json",
                  raw_path + "/ticket_line.csv",
                  raw_path + "/stores.csv",
                  raw_path + "/stores_v2.csv"]
    formats = ["json", "csv", "csv", "csv"]
    options = [{"header": "true"}, {"header": "true"}, {"header": "true"}, {"header": "true"}]
    output_path = "s3a://scrm-bucket-defined/scrm/defined/data"
    result_output_path = "s3a://scrm-bucket-resultados-challenge/scrm/results/data"

    joined_df, total_quantity_df, distinct_stores_df, grouped_stores_df, unioned_stores_df = \
        data_processor.process_data(data_paths, formats, options, output_path, result_output_path)


if __name__ == "__main__":
    main()