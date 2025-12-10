import re
from pyspark.sql.utils import AnalysisException

class SparkSQLTool:
    def __init__(self, spark):
        self.spark = spark

    def run_query(self, query):
        """
        Executes a SparkSQL query with safety mechanisms and self-healing.
        """
        # 1. Higienização (Sanitization)
        clean_query = self._sanitize_query(query)

        # 2. Trava de Volumetria (Safety)
        safe_query = self._enforce_limit(clean_query)

        try:
            # Execution
            print(f"Executing Query: {safe_query}")
            # User confirmed this syntax works: spark.sql("...").show() or toPandas
            df = self.spark.sql(safe_query)
            
            # Using toPandas() as requested/suggested by user for easier string formatting for the LLM
            # outputting a limited string representation to not blow up context
            return df.limit(20).toPandas().to_string() 
        
        except AnalysisException as e:
            # 3. Trava de Auto-Cura (Self-Healing)
            error_message = str(e)
            print(f"SQL Error Encountered: {error_message}")
            return f"ERROR: The interaction failed due to the following SQL error: {error_message}. Please analyze the error and rewrite your query using correct table names (e.g., olist_dataset.domain.table)."
        except Exception as e:
            return f"ERROR: An unexpected error occurred: {str(e)}"

    def _sanitize_query(self, query):
        """Removes markdown code blocks and extra whitespace."""
        query = re.sub(r"```sql", "", query, flags=re.IGNORECASE)
        query = query.replace("```", "").strip()
        return query

    def _enforce_limit(self, query, default_limit=10):
        """Ensures the query has a LIMIT clause to prevent data explosion."""
        # Simple check - in a real robust parser we would parse the AST
        if "limit" not in query.lower():
            return f"{query} LIMIT {default_limit}"
        return query
