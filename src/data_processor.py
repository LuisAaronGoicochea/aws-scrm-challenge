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
    
    def integrate_stores_data(self, stores_df, stores_v2_df, store_id_col, country_col, version_col):

        stores_v2_df = stores_v2_df.select(
            col(store_id_col).substr(3, 2).alias(store_id_col),
            col(store_id_col).substr(1, 2).alias(country_col),
            col(version_col)
        )

        integrated_df = stores_df.union(stores_v2_df)

        return integrated_df
    
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
    
    def group_stores_by_category(self, base_df, products_df, arguments):
        join_columns = arguments.get('join_columns')
        group_by_column = arguments.get('group_by_column')
        aggregate_column = arguments.get('aggregate_column')
        alias_name = arguments.get('alias_name')

        grouped_df = base_df \
            .join(products_df, join_columns) \
            .select(products_df["categories.category_name"].alias("category_name"), base_df["store_id"]) \
            .groupBy(group_by_column) \
            .agg(collect_list(aggregate_column).alias(alias_name))

        return grouped_df