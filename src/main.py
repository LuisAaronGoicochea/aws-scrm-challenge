import os
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from data_processor import DataProcessor
from get_keys import get_secret

# Añadiendo paquetes para obtener datos de S3
# os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages com.amazon:aws-java-sdk-s3:1.12.486,org.apache.hadoop:hadoop-aws:3.3.1 pyspark-shell"

def main():
    # Creando la configuración de Spark
    conf = SparkConf().setAppName("scrm-challenge")
    sc = SparkContext(conf=conf)

    # Creando la sesión de Spark
    spark = SparkSession(sc).builder.appName("scrm-challenge-app").getOrCreate()

    # Obtiene las variables de acceso desde Secrets Manager
    secret = get_secret()
    
    print(secret)
    
    access_key = secret['access_key']
    secret_access_key = secret['secret_key']
    
    hadoopConf = sc._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", access_key)
    hadoopConf.set("fs.s3a.secret.key", secret_access_key)
    hadoopConf.set("spark.hadoop.fs.s3a.aws.credential.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    data_processor = DataProcessor(spark)
    raw_path = "s3://scrm-challenge-raw/scrm/raw/data"
    data_paths = [raw_path + "/products.json",
                  raw_path + "/ticket_line.csv",
                  raw_path + "/stores.csv",
                  raw_path + "/stores_v2.csv"]
    formats = ["json", "csv", "csv", "csv"]
    options = [{"header": "true"}, {"header": "true"}, {"header": "true"}, {"header": "true"}]
    output_path = "s3://scrm-bucket-defined/scrm/defined/data"
    result_output_path = "s3://scrm-bucket-resultados-challenge/scrm/results/data"

    joined_df, total_quantity_df, distinct_stores_df, grouped_stores_df, unioned_stores_df = \
        data_processor.process_data(data_paths, formats, options, output_path, result_output_path)


if __name__ == "__main__":
    main()