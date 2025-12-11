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
    def __init__(self, spark):
        """
        Inicializa o ContextManager.

        Armazena a sessão Spark e define o mapeamento entre as funções dos agentes e 
        seus domínios de dados acessíveis (bancos de dados).

        Entradas:
            spark (SparkSession): O objeto de sessão Spark ativo usado para consultar metadados.

        Saídas:
            Nenhuma
        """
        self.spark = spark
        print("[ContextManager] Inicializado com sucesso!")
        # Mapping roles to their allowed domains
        # Using "olist_dataset" catalog prefix if available
        self.role_domains = {
            "logistics": ["olist_dataset.olist_sales", "olist_dataset.olist_logistics"],
            "finance": ["olist_dataset.olist_finance", "olist_dataset.olist_cx"]
        }
        # Available tables for reference
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
        Gera uma descrição de texto dos schemas para a função do agente especificada.
        
        Esta função itera pelos domínios permitidos para a função dada, 
        busca a lista de tabelas e suas definições de colunas usando Spark SQL, 
        e formata esta informação em uma string de descrição. Utiliza .toPandas() 
        para lidar com a coleta de dados de forma segura em vários ambientes Spark.

        Entradas:
            agent_role (str): A função do agente ('logistics', 'finance', 'coo').

        Saídas:
            str: Uma string formatada contendo nomes e detalhes das colunas de todas 
                 as tabelas acessíveis ao agente.
        """
        if agent_role not in self.role_domains:
            return ""

        domains = self.role_domains[agent_role]
        context_str = f"Specific Data Context for {agent_role.capitalize()} Agent:\\n"

        for domain in domains:
            try:
                # Using toPandas verifyied by user as working
                print(f"Loading schema for domain: {domain}")
                
                # List tables
                tables_df = self.spark.sql(f"SHOW TABLES IN {domain}").limit(100)
                pdf_tables = tables_df.toPandas()
                
                # Iterate using pandas 
                for index, row in pdf_tables.iterrows():
                    t_name = row['tableName']
                    # Using fully qualified name
                    full_table_name = f"{domain}.{t_name}"
                    
                    context_str += f"\\nTable: {full_table_name}\\nColumns:\\n"
                    
                    # Describe columns
                    columns_df = self.spark.sql(f"DESCRIBE {full_table_name}")
                    pdf_columns = columns_df.toPandas()
                    
                    for idx, col in pdf_columns.iterrows():
                        col_name = col["col_name"]
                        data_type = col["data_type"]
                        context_str += f" - {col_name} ({data_type})\\n"
                        
            except Exception as e:
                print(f"Error loading schema for {domain}: {e}")
                context_str += f"\\nCould not retrieve schema for domain {domain}: {str(e)}\\n"
        
        return context_str
