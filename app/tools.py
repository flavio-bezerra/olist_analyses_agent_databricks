"""
Ferramentas (As Mãos)

Aqui ficam as ferramentas físicas que os agentes usam para interagir com o mundo (neste caso, o banco de dados).

Objetivo Didático:
Os agentes são "cérebros" de texto. Eles não conseguem clicar botões ou rodar SQL sozinhos.
A classe SparkSQLTool é como um teclado seguro que damos a eles.
1. O agente diz: "Quero ver as vendas de ontem".
2. Ele gera um texto SQL.
3. Esta ferramenta pega o texto, verifica se é seguro (Sanitização), garante que não vai travar o banco (Limit) e executa no Spark.
4. Se der erro, ela avisa o agente para ele tentar de novo.
"""
import re
from pyspark.sql.utils import AnalysisException

class SparkSQLTool:
    def __init__(self, spark):
        """
        Inicializa o SparkSQLTool.

        Entradas:
            spark (SparkSession): A sessão Spark ativa usada para executar consultas.

        Saídas:
            Nenhuma
        """
        self.spark = spark

    def run_query(self, query):
        """
        Executa uma consulta SparkSQL com mecanismos de segurança e auto-cura.

        Este método higieniza a consulta de entrada, garante que um limite de linhas seja aplicado,
        executa-a usando Spark e retorna os resultados formatados como uma string.
        Ele captura exceções de análise (erros SQL) e retorna uma mensagem de erro descritiva
        para ajudar o agente a se corrigir.

        Entradas:
            query (str): A string da consulta SQL bruta para executar.

        Saídas:
            str: Os resultados da consulta (top 20 linhas) como uma string, ou uma mensagem de erro
                 começando com "ERROR:".
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
        """
        Limpa a string da consulta de entrada.

        Remove formatação markdown (ex: blocos ```sql), remove espaços em branco extras
        e remove ponto e vírgula no final para prevenir erros de sintaxe na execução.

        Entradas:
            query (str): A string da consulta de entrada bruta.

        Saídas:
            str: A string da consulta limpa e executável.
        """
        query = re.sub(r"```sql", "", query, flags=re.IGNORECASE)
        query = query.replace("```", "").strip()
        # Remove trailing semicolon if present, as it breaks spark.sql when we append LIMIT
        if query.endswith(";"):
            query = query[:-1].strip()
        return query

    def _enforce_limit(self, query, default_limit=10):
        """
        Garante que a consulta contenha uma cláusula LIMIT.

        Verifica se a consulta já contém 'LIMIT' (case-insensitive). Se não,
        adiciona uma cláusula de limite padrão para prevenir a extração excessiva de dados.

        Entradas:
            query (str): A string da consulta SQL limpa.
            default_limit (int, opcional): O limite de linhas a adicionar se estiver faltando. Padrão é 10.

        Saídas:
            str: A string da consulta com uma cláusula LIMIT aplicada.
        """
        # Simple check - in a real robust parser we would parse the AST
        if "limit" not in query.lower():
            return f"{query} LIMIT {default_limit}"
        return query
