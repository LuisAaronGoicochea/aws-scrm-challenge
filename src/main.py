from pyspark.sql.functions import *
from data_processor import DataProcessor

def main():
    spark = SparkSession.builder.getOrCreate()
    data_processor = DataProcessor(spark)
    raw_path = "s3://scrm-challenge-raw/scrm/raw/data"
    data_paths = [raw_path + "/products.json",
                  raw_path + "/ticket_line.csv",
                  raw_path + "/stores.csv",
                  raw_path + "/stores_v2.csv"]
    formats = ["json", "csv", "csv", "csv]
    options = [{"header": "true"}, {"header": "true"}, {"header": "true"}, {"header": "true"}]
    output_path = "s3://scrm-bucket-defined/scrm/defined/data"
    result_output_path = "s3://scrm-bucket-resultados-challenge/scrm/results/data"

    joined_df, total_quantity_df, distinct_stores_df, grouped_stores_df, unioned_stores_df = \
        data_processor.process_data(data_paths, formats, options, output_path, result_output_path)


if __name__ == "__main__":
    main()