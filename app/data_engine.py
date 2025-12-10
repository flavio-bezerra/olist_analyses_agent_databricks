from pyspark.sql import SparkSession
import os

class DataEngine:
    def __init__(self, data_path="data/"):
        self.spark = SparkSession.builder \
            .appName("OlistDataMesh") \
            .enableHiveSupport() \
            .getOrCreate()
        self.data_path = data_path

    def initialize_session(self):
        """Just ensures Spark is alive."""
        return self.spark

    def ingest_data_mesh(self):
        """
        Creates the 4 logical databases (Domínios de Dados) and loads data.
        Call this EXPERICITLY when you want to re-load raw CSVs into Tables.
        """
        domains = {
            "olist_sales": ["olist_orders_dataset.csv", "olist_order_items_dataset.csv", "olist_products_dataset.csv"],
            "olist_logistics": ["olist_sellers_dataset.csv", "olist_geolocation_dataset.csv", "olist_customers_dataset.csv"],
            "olist_finance": ["olist_order_payments_dataset.csv"],
            "olist_cx": ["olist_order_reviews_dataset.csv"]
        }

        for domain, files in domains.items():
            print(f"Creating Domain: {domain}")
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {domain}")
            
            for file in files:
                table_name = file.replace("olist_", "").replace("_dataset.csv", "")
                file_path = os.path.join(self.data_path, file)
                
                # Check if file exists to avoid errors during empty runs
                if os.path.exists(file_path):
                    print(f"  Loading {table_name} from {file_path} into {domain}.{table_name}")
                    try:
                        df = self.spark.read.csv(file_path, header=True, inferSchema=True)
                        # Save as table in the specific database
                        df.write.mode("overwrite").saveAsTable(f"{domain}.{table_name}")
                    except Exception as e:
                        print(f"  Error loading {table_name}: {e}")
                else:
                    print(f"  Warning: File {file_path} not found. Skipping table {domain}.{table_name}.")

    def get_spark_session(self):
        return self.spark
