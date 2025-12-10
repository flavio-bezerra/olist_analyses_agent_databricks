class ContextManager:
    def __init__(self, spark):
        self.spark = spark
        # Mapping roles to their allowed domains
        # Using "olist_dataset" catalog prefix if available
        self.role_domains = {
            "logistics": ["olist_dataset.olist_sales", "olist_dataset.olist_logistics"],
            "finance": ["olist_dataset.olist_finance", "olist_dataset.olist_cx"]
        }
        # Available tables for reference
        self.available_tables = [
            ("olist_cx", "order_reviews", False),
            ("olist_finance", "order_payments", False),
            ("olist_logistics", "customers", False),
            ("olist_logistics", "geolocation", False),
            ("olist_logistics", "sellers", False),
            ("olist_sales", "order_items", False),
            ("olist_sales", "orders", False),
            ("olist_sales", "products", False)
        ]

    def get_schema_context(self, agent_role):
        """
        Generates a text description of the schemas for the specified agent role.
        """
        if agent_role not in self.role_domains:
            return ""

        domains = self.role_domains[agent_role]
        context_str = f"Specific Data Context for {agent_role.capitalize()} Agent:\n"

        for domain in domains:
            try:
                print(f"Loading schema for domain: {domain}")
                tables_df = self.spark.sql(f"SHOW TABLES IN {domain}").limit(100)
                tables = tables_df.select("tableName").rdd.flatMap(lambda x: x).collect()
                
                for t_name in tables:
                    full_table_name = f"{domain}.{t_name}"
                    context_str += f"\nTable: {full_table_name}\nColumns:\n"
                    columns_df = self.spark.sql(f"DESCRIBE {full_table_name}")
                    columns = columns_df.select("col_name", "data_type").rdd.collect()
                    for col in columns:
                        col_name = col["col_name"]
                        data_type = col["data_type"]
                        context_str += f" - {col_name} ({data_type})\n"
            except Exception as e:
                print(f"Error loading schema for {domain}: {e}")
                context_str += f"\nCould not retrieve schema for domain {domain}: {str(e)}\n"

        domains = self.role_domains[agent_role]
        context_str = f"Specific Data Context for {agent_role.capitalize()} Agent:\\n"

        for domain in domains:
            try:
                # User verified syntax: spark.sql("SHOW TABLES IN olist_dataset.olist_cx")
                # Using toPandas() to avoid socket errors with .collect()
                print(f"Loading schema for domain: {domain}")
                tables_df = self.spark.sql(f"SHOW TABLES IN {domain}").limit(100)
                # Converting to pandas to avoid socket issues seen with .collect() in some setups
                pdf_tables = tables_df.toPandas()
                
                for index, row in pdf_tables.iterrows():
                    # columns usually: database, tableName, isTemporary
                    t_name = row['tableName']
                    # Construct full qualified name: olist_dataset.olist_finance.table_name
                    full_table_name = f"{domain}.{t_name}"
                    
                    context_str += f"\\nTable: {full_table_name}\\nColumns:\\n"
                    
                    # Describe
                    columns_df = self.spark.sql(f"DESCRIBE {full_table_name}")
                    pdf_columns = columns_df.toPandas()
                    
                    for idx, col in pdf_columns.iterrows():
                        col_name = col['col_name']
                        data_type = col['data_type']
                        context_str += f" - {col_name} ({data_type})\\n"
            except Exception as e:
                # Fallback implementation logic remains or simplified
                print(f"Error loading schema for {domain}: {e}")
                context_str += f"\\nCould not retrieve schema for domain {domain}: {str(e)}\\n"
