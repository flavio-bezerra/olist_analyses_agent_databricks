class ContextManager:
    def __init__(self, spark):
        self.spark = spark
        # Mapping roles to their allowed domains
        # Using "olist_dataset" catalog prefix if available
        self.role_domains = {
            "logistics": ["olist_dataset.olist_sales", "olist_dataset.olist_logistics"],
            "finance": ["olist_dataset.olist_finance", "olist_dataset.olist_cx"]
        }

    def get_schema_context(self, agent_role):
        """
        Generates a text description of the schemas for the specified agent role.
        """
        if agent_role not in self.role_domains:
            return ""

        domains = self.role_domains[agent_role]
        context_str = f"Specific Data Context for {agent_role.capitalize()} Agent:\\n"

        for domain in domains:
            try:
                # User verified syntax: spark.sql("SHOW TABLES IN olist_dataset.olist_cx")
                # Our 'domain' variable already contains 'olist_dataset.olist_finance' etc from role_domains
                print(f"Loading schema for domain: {domain}")
                tables_df = self.spark.sql(f"SHOW TABLES IN {domain}")
                tables = tables_df.collect() 
                
                for table in tables:
                    # columns usually: database, tableName, isTemporary
                    t_name = table['tableName']
                    # Construct full qualified name: olist_dataset.olist_finance.table_name
                    full_table_name = f"{domain}.{t_name}"
                    
                    context_str += f"\\nTable: {full_table_name}\\nColumns:\\n"
                    
                    # Describe
                    columns_df = self.spark.sql(f"DESCRIBE {full_table_name}")
                    columns = columns_df.collect()
                    
                    for col in columns:
                        col_name = col['col_name']
                        data_type = col['data_type']
                        context_str += f" - {col_name} ({data_type})\\n"
            except Exception as e:
                # Fallback implementation logic remains or simplified
                print(f"Error loading schema for {domain}: {e}")
                context_str += f"\\nCould not retrieve schema for domain {domain}: {str(e)}\\n"
