import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc

if __name__ == "__main__":

    base_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    silver_path = os.path.join(base_path, 'lakehouse', 'silver')
    output_file_path = os.path.join(base_path, 'lakehouse', 'gold', 'dataset.parquet')

    spark = SparkSession.builder.appName("DatasetGold").getOrCreate()

    try:
        customers_df = spark.read.parquet(os.path.join(silver_path, 'customers.parquet'))
        orders_df = spark.read.parquet(os.path.join(silver_path, 'orders.parquet'))
        order_item_df = spark.read.parquet(os.path.join(silver_path, 'order_item.parquet'))

        orders_customers_df = orders_df.join(
            customers_df,
            orders_df["customer_id"] == customers_df["id"],
            "inner"
        ).select(
            orders_df["id"].alias("order_id"),
            customers_df["city"],
            customers_df["state"]
        )

        orders_customers_df.show()

        dataset_gold_df = orders_customers_df.join(
            order_item_df,
            orders_customers_df["order_id"] == order_item_df["order_id"],
            "inner"
        ).groupBy("city", "state").agg(
            sum("quantity").alias("quantity_orders"),
            sum("subtotal").alias("total")
        ).select(
            "city",
            "state",
            "quantity_orders",
            "total"
        ).orderBy(desc("total"))

        dataset_gold_df.printSchema()
        dataset_gold_df.show()
        
        dataset_gold_df.write.mode("overwrite").parquet(output_file_path)
        
    except Exception as e:
        print(f"Erro ao processar: {e}")

    finally:
        spark.stop()