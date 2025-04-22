import os
from pyspark.sql import SparkSession

if __name__ == "__main__":

    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    parquet_file_path = os.path.join(base_path, 'lakehouse', 'bronze', 'orders.parquet')
    output_file_path = os.path.join(base_path, 'lakehouse', 'silver', 'orders.parquet')

    spark = SparkSession.builder.appName("RenameOrderColumns").getOrCreate()

    try:
        df = spark.read.parquet(parquet_file_path)

        df_renamed = df
        for col in df.columns:
            if col.startswith("order_"):
                new_col = col.replace("order_", "")
                df_renamed = df_renamed.withColumnRenamed(col, new_col)

        df_renamed.write.mode("overwrite").parquet(output_file_path)

        # conferir parquet:
        df_parquet = spark.read.parquet(output_file_path)

        print("conferir novo parquet:")

        df_parquet.show(5)

    except Exception as e:
        print(f"Erro ao processar '{parquet_file_path}': {e}")

    finally:
        spark.stop()