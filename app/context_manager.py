class ContextManager:
    def __init__(self, spark):
        self.spark = spark
        # Mapping roles to their allowed domains
        self.role_domains = {
            "logistics": ["olist_sales", "olist_logistics"],
            "finance": ["olist_finance", "olist_cx"]
        }

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
                # List tables in the database
                tables = self.spark.catalog.listTables(domain)
                for table in tables:
                    table_name = f"{domain}.{table.name}"
                    context_str += f"\nTable: {table_name}\nColumns:\n"
                    
                    # Describe table to get columns
                    # Using SQL mostly because listColumns might need specific handling
                    columns_df = self.spark.sql(f"DESCRIBE {table_name}")
                    columns = columns_df.collect()
                    
                    for col in columns:
                        # usually col_name, data_type, comment
                        col_name = col['col_name']
                        data_type = col['data_type']
                        context_str += f" - {col_name} ({data_type})\n"
            except Exception as e:
                context_str += f"\nCould not retrieve schema for domain {domain}: {str(e)}\n"
        
        return context_str
