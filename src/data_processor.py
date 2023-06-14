from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from itertools import chain

class DataProcessor:
    def __init__(self, spark):
        self.spark = spark

    def read_data(self, format, path, options=None):
        """
        Lee los datos de un archivo en el formato especificado y ruta dada.

        Args:
            format (str): Formato de los datos (ej. "json", "csv").
            path (str): Ruta del archivo de datos.
            options (dict, optional): Opciones adicionales para la lectura de datos. Default es None.

        Retorna:
            pyspark.sql.DataFrame: DataFrame que contiene los datos leídos.
        """
        if options is None:
            options = {}

        if isinstance(options, list):
            # Combina los diccionarios en la lista de opciones
            merged_options = dict(chain.from_iterable(d.items() for d in options))
            options = merged_options

        return self.spark.read.format(format).options(**options).load(path)
    
    def update_dataframe(self, base_df, new_df, select_columns, join_columns):
        new_df = new_df.select(*select_columns)
        return base_df.union(new_df)
    
    def distinct_stores(self, base_df, join_columns, group_by_columns, distinct_count_column, result_column):
        distinct_stores_df = base_df \
            .join(base_df, join_columns) \
            .groupBy(group_by_columns) \
            .agg(countDistinct(distinct_count_column).alias(result_column))
        return distinct_stores_df
    
    def calculate_second_most_selling(self, base_df, arguments):
        join_columns = arguments.get('join_columns')
        group_by_columns = arguments.get('group_by_columns')
        quantity_column = arguments.get('quantity_column')
        rank_column = arguments.get('rank_column')
        select_columns = arguments.get('select_columns')

        window_spec = Window.partitionBy(group_by_columns).orderBy(desc(quantity_column))

        ranked_df = base_df \
            .groupBy(*join_columns) \
            .agg(sum(quantity_column).alias(quantity_column)) \
            .withColumn(rank_column, row_number().over(window_spec))

        second_most_selling_df = ranked_df \
            .filter(col(rank_column) == 2) \
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