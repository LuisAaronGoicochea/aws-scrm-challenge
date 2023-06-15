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
    conf.set("spark.hadoop.fs.s3a.access.key", access_key)
    conf.set("spark.hadoop.fs.s3a.secret.key", secret_access_key)
    conf.set("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Creando el contexto de Spark
    sc = SparkContext(conf=conf)
    
    # Creando la sesión de Spark
    spark = SparkSession(sc).builder.appName("scrm-challenge-app").getOrCreate()

    data_processor = DataProcessor(spark)
    raw_path= "s3://scrm-challenge-raw/scrm/raw/data"
    data_paths = [raw_path + "/products.json",
                  raw_path + "/ticket_line.csv",
                  raw_path + "/stores.csv"]
                  
    formats = ["json", "csv", "csv"]
    options = [{"header": "true"}, {"header": "true"}, {"header": "true"}]
    
    result_output_path = "s3://scrm-bucket-resultados-challenge/scrm/results/data"

    product_df, ticket_line_df, stores_df = data_processor.read_data(formats,  data_paths, options)
    
    # Ejercicio 1:
    
    # Definir las columnas para unir, agrupar y contar valores distintos, y el nombre de la columna de resultado
    join_columns = ["product_id"]
    group_by_columns = []
    distinct_count_column = "store_id"
    result_column = "num_stores"

    # Realizar las operaciones de forma secuencial
    distinct_stores_df = data_processor.distinct_stores(ticket_line_df, join_columns, group_by_columns, distinct_count_column, result_column)

    # Exportar el DataFrame resultante a la capa Defined del bucket de S3 (se define una función coalesce en 1 para que guarde un solo archivo en el bucket.
    data_processor.write_spark_df_to_s3_with_specific_file_name(distinct_stores_df, result_output_path, "/1_distinct_stores_df.csv", True)
    
    # Ejercicio 2:
    
    # Definir los nombres de las columnas y los dataframes correspondientes
    join_columns = ["store_id"]
    group_by_columns = ["product_id", "store_id"]
    quantity_column = "quantity"
    rank_column = "rank"
    select_columns = ["product_id", "store_id", "total_quantity"]
    
    # Realizar las operaciones de forma secuencial
    second_most_selling_df = data_processor.calculate_second_most_selling(ticket_line_df, stores_df, join_columns, group_by_columns, quantity_column, rank_column, select_columns)
    
    # Exportar el DataFrame resultante a la capa Defined del bucket de S3
    data_processor.write_spark_df_to_s3_with_specific_file_name(second_most_selling_df, result_output_path, "/2_second_most_selling_df.csv", True)
    """
    # Ejercicio 3:
    
    # Definir los argumentos para la función group_stores_by_category
    arguments = {
        'join_columns': secondMostSellingDF["product_id"] == productsDF["product_id"],
        'select_columns': [productsDF["categories.category_name"].alias("category_name"), secondMostSellingDF["store_id"]],
        'group_by_column': "category_name",
        'aggregate_column': "store_id",
        'alias_name': "stores"
    }

    # Realizar las operaciones de forma secuencial
    grouped_stores_df = data_processor.group_stores_by_category(secondMostSellingDF, arguments)

    # Exportar el DataFrame resultante a la capa Defined del bucket de S3
    grouped_stores_df.write.option("header", True).csv(result_output_path + "/3_grouped_stores_df.csv", header=True)
    
    # Ejercicio 4:
    
    # Leer el archivo CSV "stores_v2.csv"
    stores_v2_path = raw_path + "/stores_v2.csv"
    
    # Definir las columnas a seleccionar y las columnas para unir los DataFrames
    
    select_columns = [
        col("store_id").substr(3, 2).alias("store_id"),
        col("store_id").substr(1, 2).alias("country"),
        "version"
    ]
    
    # Definir las columnas para hacer la unión. En caso no tenga, dejar en blanco
    join_columns = []
    
    updated_stores_df = data_processor.update_dataframe(stores_df, stores_v2_df, select_columns, join_columns)
    
    # Exportar el DataFrame resultante a la capa Defined del bucket de S3
    unioned_stores_df.write.option("header", True).csv(result_output_path + "/4_unioned_stores_df.csv", header=True)"""

if __name__ == "__main__":
    main()