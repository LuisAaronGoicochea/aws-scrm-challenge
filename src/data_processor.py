import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from itertools import chain
from urllib.parse import urlparse


class DataProcessor:
    def __init__(self, spark):
        self.spark = spark

    def read_data(self, formats, paths, options=None):
        if options is None:
            options = {}

        dfs = []
        for format, path, opts in zip(formats, paths, options):
            df = self.spark.read.format(format)
            for key, value in opts.items():
                df = df.option(key, value)
            dfs.append(df.load(path))

        return tuple(dfs)
    
    def write_spark_df_to_s3_with_specific_file_name(self, df, output_path, header_state):
        # Reparticionar y escribir spark dataframe en S3
        df.repartition(1).write.mode("append").format(output_path.split(".")[-1]). \
            option("header", header_state). \
            save("/".join(output_path.split("/")[0:-1]))

        # Extraer nombre de bucket y clave dada una ruta de archivo S3
        s3_path = urlparse(output_path, allow_fragments=False)
        bucket_name, key = s3_path.netloc, s3_path.path.lstrip("/")

        # Renombrar los archivos particionados
        try:
            s3 = boto3.client('s3')
            prefix = "/".join(key.split("/")[0:-1]) + "/part"
            objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']
            for obj in objects:
                old_key = obj['Key']
                new_key = old_key.replace("/part", "")
                s3.copy_object(Bucket=bucket_name, CopySource=f"{bucket_name}/{old_key}", Key=new_key)
                s3.delete_object(Bucket=bucket_name, Key=old_key)
        except Exception as err:
            raise Exception("Error renaming the part files in {}: {}".format(output_path, err))
    
    def update_dataframe(self, base_df, new_df, select_columns, join_columns):
        new_df = new_df.select(*select_columns)
        return base_df.union(new_df)
    
    def distinct_stores(self, base_df, join_columns, group_by_columns, distinct_count_column, result_column):
        distinct_stores_df = base_df \
            .groupBy(*join_columns, *group_by_columns) \
            .agg(count(distinct_count_column).alias(result_column))
        return distinct_stores_df
    
    def calculate_second_most_selling(self, base_df, join_df, join_columns, group_by_columns, quantity_column, rank_column, select_columns):
        # Definición de la ventana de partición y ordenación
        window_spec = Window.partitionBy(base_df.product_id).orderBy(desc("total_quantity"))

        # Agregación por group_by_columns, y suma de quantity_column
        aggregated_df = base_df.join(join_df, join_columns) \
            .groupBy(*group_by_columns) \
            .agg(sum(col(quantity_column)).alias("total_quantity"))

        # Asignación de ranking a los registros dentro de cada partición
        ranked_df = aggregated_df.withColumn(rank_column, row_number().over(window_spec))

        # Filtrado del segundo registro más vendido
        second_most_selling_df = ranked_df.filter(ranked_df.rank == 2) \
            .select(*select_columns)

        return second_most_selling_df
    
    def group_stores_by_category(self, base_df, arguments):
        join_columns = arguments.get('join_columns')
        select_columns = arguments.get('select_columns')
        group_by_column = arguments.get('group_by_column')
        aggregate_column = arguments.get('aggregate_column')
        alias_name = arguments.get('alias_name')

        grouped_df = base_df \
            .join(productsDF, join_columns) \
            .select(*select_columns) \
            .groupBy(group_by_column) \
            .agg(collect_list(aggregate_column).alias(alias_name))

        return grouped_df