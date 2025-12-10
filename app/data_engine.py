from pyspark.sql import SparkSession
import os

class DataEngine:
    def __init__(self, data_path="data/"):
        self.data_path = data_path
        # Do not initialize spark here to avoid conflict in Databricks
        self.spark = None

    def initialize_session(self):
        """
        Gets the existing Spark session. 
        In Databricks, this picks up the global 'spark' variable context.
        """
        if self.spark is None:
            # Try to get existing session (Databricks default)
            self.spark = SparkSession.getActiveSession()
            
            # Fallback for local testing ONLY if no session exists
            if self.spark is None:
                self.spark = SparkSession.builder \
                    .appName("OlistDataMesh") \
                    .getOrCreate()
        
        return self.spark

    def get_spark_session(self):
        # Lazy load ensures we don't crash on import/init
        if self.spark is None:
            return self.initialize_session()
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

        # Try to create catalog
        try:
             self.spark.sql("CREATE CATALOG IF NOT EXISTS olist_dataset")
             catalog_prefix = "olist_dataset."
        except:
             print("Warning: Could not create catalog. Using default.")
             catalog_prefix = ""

        for domain, files in domains.items():
            db_name = f"{catalog_prefix}{domain}"
            print(f"Creating Domain: {db_name}")
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            
            for file in files:
                table_name = file.replace("olist_", "").replace("_dataset.csv", "")
                file_path = os.path.join(self.data_path, file)
                
                # Check if file exists to avoid errors during empty runs
                if os.path.exists(file_path):
                    target_table = f"{db_name}.{table_name}"
                    print(f"  Loading {table_name} from {file_path} into {target_table}")
                    try:
                        df = self.spark.read.csv(file_path, header=True, inferSchema=True)
                        # Save as table in the specific database
                        df.write.mode("overwrite").saveAsTable(target_table)
                    except Exception as e:
                        print(f"  Error loading {table_name}: {e}")
                else:
                    print(f"  Warning: File {file_path} not found. Skipping table.")

    def get_spark_session(self):
        return self.spark
