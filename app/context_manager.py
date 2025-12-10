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
                # Use SQL to list tables which handles 'catalog.database' better than catalog.listTables("db") in some versions
                # or just use spark.catalog.listTables(dbName) if updated.
                # Let's use SQL for broader compatibility with Hive/Unity Catalog syntax
                tables_df = self.spark.sql(f"SHOW TABLES IN {domain}")
                tables = tables_df.collect() # columns: database, tableName, isTemporary
                
                for table in tables:
                    # In SHOW TABLES, usually column 'tableName' holds the name
                    # Warning: 'database' column might be empty if we used fully qualified, or it matches the db
                    t_name = table['tableName']
                    full_table_name = f"{domain}.{t_name}"
                    
                    context_str += f"\\nTable: {full_table_name}\\nColumns:\\n"
                    
                    # Describe table to get columns
                    columns_df = self.spark.sql(f"DESCRIBE {full_table_name}")
                    columns = columns_df.collect()
                    
                    for col in columns:
                        # usually col_name, data_type, comment
                        col_name = col['col_name']
                        data_type = col['data_type']
                        context_str += f" - {col_name} ({data_type})\\n"
            except Exception as e:
                # Fallback implementation if catalog 'olist_dataset' doesn't exist (e.g. running local w/o UC)
                # We try removing the prefix 'olist_dataset.'
                fallback_domain = domain.replace("olist_dataset.", "")
                if fallback_domain != domain:
                     try:
                        tables_df = self.spark.sql(f"SHOW TABLES IN {fallback_domain}")
                        tables = tables_df.collect()
                        pass # If successful, we should process it. 
                        # For simplicity, we just append a note or recursively call? 
                        # Let's just error message here to keep it simple or log the fallback attempt.
                        context_str += f"\\n(Fallback) Could not reach {domain}, trying {fallback_domain}...\\n"
                        # Re-implementing the loop for fallback - duplicating code slightly to be safe
                        for table in tables:
                            t_name = table['tableName']
                            full_table_name = f"{fallback_domain}.{t_name}"
                            context_str += f"\\nTable: {full_table_name}\\nColumns:\\n"
                            columns_df = self.spark.sql(f"DESCRIBE {full_table_name}")
                            columns = columns_df.collect()
                            for col in columns:
                                context_str += f" - {col['col_name']} ({col['data_type']})\\n"
                     except Exception as e2:
                        context_str += f"\\nCould not retrieve schema for domain {domain}: {str(e)}\\n"
                else:
                    context_str += f"\\nCould not retrieve schema for domain {domain}: {str(e)}\\n"
        
        return context_str
