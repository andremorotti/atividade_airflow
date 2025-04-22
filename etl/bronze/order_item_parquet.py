import os
from pyspark.sql import SparkSession

if __name__ == "__main__":

    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    landing_file_path = os.path.join(base_path, 'lakehouse', 'landing', 'order_item.json')
    bronze_dir_path = os.path.join(base_path, 'lakehouse', 'bronze')
    output_file_path = os.path.join(bronze_dir_path, 'order_item.parquet')

    spark = SparkSession.builder.appName("BronzeOrderItem").getOrCreate()

    try:
        df_json = spark.read.json(landing_file_path)

        print("conferir json:")

        df_json.printSchema()

        df_json.show(5)

        df_json.write.mode("overwrite").parquet(output_file_path)
        

        # conferir parquet:
        df_parquet = spark.read.parquet(output_file_path)

        print("conferir parquet:")

        df_parquet.printSchema()

        df_parquet.show(5)

    except Exception as e:
        print(f"Erro ao processar '{landing_file_path}': {e}")
        
    finally:
        spark.stop()