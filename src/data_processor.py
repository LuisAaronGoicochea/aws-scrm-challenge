from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from itertools import chain

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
    
    def update_dataframe(self, base_df, new_df, select_columns, join_columns):
        new_df = new_df.select(*select_columns)
        return base_df.union(new_df)
    
    def distinct_stores(self, base_df, join_columns, group_by_columns, distinct_count_column, result_column):
        distinct_stores_df = base_df \
            .groupBy(*join_columns, *group_by_columns) \
            .agg(count(distinct_count_column).alias(result_column))
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