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

        for domain in domains:
            try:
                print(f"Loading schema for domain: {domain}")
                tables_df = self.spark.sql(f"SHOW TABLES IN {domain}").limit(100)
                pdf_tables = tables_df.toPandas()

                for index, row in pdf_tables.iterrows():
                    t_name = row['tableName']
                    full_table_name = f"{domain}.{t_name}"

                    # Buscar comentário da tabela
                    table_comment = ""
                    try:
                        table_info_df = self.spark.sql(
                            f"SELECT comment FROM {self.catalog}.information_schema.tables WHERE table_catalog='{self.catalog}' AND table_schema='{domain.split('.')[-1]}' AND table_name='{t_name}'"
                        )
                        table_info = table_info_df.toPandas()
                        if not table_info.empty and table_info.loc[0, "comment"]:
                            table_comment = table_info.loc[0, "comment"]
                    except Exception as e:
                        table_comment = f"Erro ao buscar comentário da tabela: {e}"

                    context_str += f"\nTable: {full_table_name}\n"
                    if table_comment:
                        context_str += f"Definition: {table_comment}\n"
                    context_str += "Columns:\n"

                    # Buscar colunas e comentários
                    columns_df = self.spark.sql(
                        f"SELECT column_name, data_type, comment FROM {self.catalog}.information_schema.columns WHERE table_catalog='{self.catalog}' AND table_schema='{domain.split('.')[-1]}' AND table_name='{t_name}'"
                    )
                    pdf_columns = columns_df.toPandas()

                    for idx, col in pdf_columns.iterrows():
                        col_name = col["column_name"]
                        data_type = col["data_type"]
                        col_comment = col["comment"] if col["comment"] else ""
                        context_str += f" - {col_name} ({data_type})"
                        if col_comment:
                            context_str += f": {col_comment}"
                        context_str += "\n"

            except Exception as e:
                print(f"Error loading schema for {domain}: {e}")
                context_str += f"\nCould not retrieve schema for domain {domain}: {str(e)}\n"

        return context_str