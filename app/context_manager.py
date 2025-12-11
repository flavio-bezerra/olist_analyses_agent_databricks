"""
Gerenciador de Contexto (O Mapa)

Este módulo atua como o bibliotecário do sistema. Sua função é fornecer aos agentes 
as informações corretas sobre o que existe no banco de dados.

Objetivo Didático:
Modelos de IA (LLMs) não "adivinham" os nomes das tabelas ou colunas. 
O ContextManager olha para o Spark, lê o esquema das tabelas (quais colunas existem, tipos de dados) 
e cria um "resumo" para o agente. 
Ex: "Agente de Logística, você tem acesso à tabela 'orders' com as colunas 'order_id' e 'delivery_date'."
"""
class ContextManager:
    def __init__(self, spark, catalog="olist_dataset"):
        """
        Inicializa o ContextManager.

        Armazena a sessão Spark e define o mapeamento entre as funções dos agentes e 
        seus domínios de dados acessíveis (bancos de dados).

        Entradas:
            spark (SparkSession): O objeto de sessão Spark ativo usado para consultar metadados.
            catalog (str): Nome do catálogo a ser utilizado.

        Saídas:
            Nenhuma
        """
        self.spark = spark
        self.catalog = catalog
        print("[ContextManager] Inicializado com sucesso!")
        # Mapping roles to their allowed domains
        self.role_domains = {
            "logistics": [f"{self.catalog}.olist_sales", f"{self.catalog}.olist_logistics"],
            "finance": [f"{self.catalog}.olist_finance", f"{self.catalog}.olist_cx"]
        }
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
        Gera uma descrição de texto dos schemas para a função do agente especificada,
        incluindo comentários das tabelas e colunas.

        Entradas:
            agent_role (str): A função do agente ('logistics', 'finance', 'coo').

        Saídas:
            str: Uma string formatada contendo nomes, detalhes e comentários das colunas de todas 
                 as tabelas acessíveis ao agente.
        """
        if agent_role not in self.role_domains:
            return ""

        domains = self.role_domains[agent_role]
        context_str = f"Specific Data Context for {agent_role.capitalize()} Agent:\n"

        input_schema_patterns = []
        for domain in domains:
            # domain is like "olist_dataset.olist_sales" -> schema "olist_sales"
            schema_name = domain.split('.')[-1]
            input_schema_patterns.append(f"'{schema_name}'")
        
        allowed_schemas_str = ", ".join(input_schema_patterns)
        
        # User requested query structure optimized for specific schemas
        query = f"""
        SELECT 
          t.table_schema as nome_do_schema,
          t.table_name as nome_da_tabela,
          t.comment as comentario_da_tabela,
          c.column_name as nome_da_coluna,
          c.data_type as tipo_da_coluna,
          c.comment as comentario_da_coluna
        FROM {self.catalog}.information_schema.tables t
        LEFT JOIN {self.catalog}.information_schema.columns c
          ON t.table_catalog = c.table_catalog
          AND t.table_schema = c.table_schema
          AND t.table_name = c.table_name
        WHERE t.table_schema IN ({allowed_schemas_str})
        ORDER BY t.table_schema, t.table_name, c.ordinal_position
        """

        try:
            print(f"Loading schema context with single query for {agent_role}...")
            schema_df = self.spark.sql(query).toPandas()
            
            current_table = None
            
            for index, row in schema_df.iterrows():
                # Construct full table name
                table_schema = row['nome_do_schema']
                table_name = row['nome_da_tabela']
                full_table_name = f"{self.catalog}.{table_schema}.{table_name}"
                
                # If we encountered a new table, print its header info
                if full_table_name != current_table:
                    current_table = full_table_name
                    table_comment = row['comentario_da_tabela']
                    
                    context_str += f"\nTable: {full_table_name}\n"
                    if table_comment:
                        context_str += f"Definition: {table_comment}\n"
                    context_str += "Columns:\n"
                
                # Add column info
                col_name = row['nome_da_coluna']
                col_type = row['tipo_da_coluna']
                col_comment = row['comentario_da_coluna']
                
                context_str += f" - {col_name} ({col_type})"
                if col_comment:
                    context_str += f": {col_comment}"
                context_str += "\n"

        except Exception as e:
            print(f"Error executing schema query: {e}")
            context_str += f"\nCould not retrieve schema context: {str(e)}\n"

        return context_str