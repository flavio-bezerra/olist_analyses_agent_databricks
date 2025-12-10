"""
Módulo de Agentes Inteligentes (O Cérebro)

Este módulo é responsável por definir a "inteligência" do sistema. Ele contém:
1. LLMClient: Uma ponte que conecta nosso código aos modelos de linguagem do Databricks (ex: Llama 3).
2. Agent: A classe que cria os "funcionários digitais" (Logística, Finanças, COO). 

Pense neste arquivo como o escritório onde os agentes "pensam", recebem tarefas, consultam ferramentas 
e geram suas respostas. Cada agente tem uma personalidade (role) e acesso a um contexto específico.
"""
from databricks_langchain import ChatDatabricks
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
import os

class LLMClient:
    def __init__(self):
        """
        Inicializa o LLMClient com um modelo Databricks específico.
        
        Este cliente configura o modelo ChatDatabricks com parâmetros pré-definidos 
        como temperatura e max tokens. Seleciona 'databricks-llama-4-maverick' 
        por padrão (índice 2).

        Entradas:
            Nenhuma

        Saídas:
            Nenhuma (inicializa self.chat_model)
        """
        # Lista de modelos disponíveis no Databricks
        models = [
            'databricks-gpt-oss-20b',
            'databricks-gpt-oss-120b',
            'databricks-llama-4-maverick',
            'databricks-gemma-3-12b',
            'databricks-meta-llama-3-1-8b-instruct',
            'databricks-meta-llama-3-3-70b-instruct',
            'databricks-gte-large-en',
            'databricks-meta-llama-3-1-405b-instruct'
        ]
        
        # Criar uma instância do modelo ChatDatabricks
        # O usuário selecionou o index 2: databricks-llama-4-maverick
        self.chat_model = ChatDatabricks(
            endpoint=models[5],  
            temperature=0.1,
            max_tokens=5000
        )

    def completion(self, messages_list):
        """
        Envia mensagens para o LLM do Databricks e retorna o conteúdo da resposta.
        
        Converte a lista de dicionários (formato OpenAI) para objetos Message do LangChain 
        e invoca o modelo de chat do Databricks.

        Entradas:
            messages_list (list): Uma lista de dicionários representando o histórico do chat.
                                  Cada dict deve ter 'role' ('system', 'user', 'assistant') 
                                  e 'content' (str).

        Saídas:
            str: O conteúdo em texto da resposta da IA. 
                 Retorna uma mensagem de erro se ocorrer uma exceção.
        """
        langchain_messages = []
        for msg in messages_list:
            role = msg.get("role")
            content = msg.get("content")
            
            if role == "system":
                langchain_messages.append(SystemMessage(content=content))
            elif role == "user":
                langchain_messages.append(HumanMessage(content=content))
            elif role == "assistant":
                langchain_messages.append(AIMessage(content=content))
            else:
                # Fallback for other roles if any
                langchain_messages.append(HumanMessage(content=content))
        
        try:
            response = self.chat_model.invoke(langchain_messages)
            # response is an AIMessage object
            return response.content
        except Exception as e:
            return f"ERROR: Databricks LLM Interaction Failed: {str(e)}"

class Agent:
    def __init__(self, name, role, context_manager, tool=None):
        """
        Inicializa um Agente com uma função, contexto e ferramenta opcionais específicos.

        Entradas:
            name (str): O nome do agente (ex: "LogisticsAgent").
            role (str): A função do agente ('logistics', 'finance', 'coo').
            context_manager (ContextManager): Um gerenciador para recuperar informações de schema e contexto.
            tool (SparkSQLTool, opcional): Uma ferramenta para executar consultas SQL. Padrão é None.

        Saídas:
            Nenhuma
        """
        self.name = name
        self.role = role # 'logistics', 'finance', 'coo'
        self.context_manager = context_manager
        self.tool = tool
        self.llm = LLMClient()
        self.history = []

    def run(self, task_input):
        """
        Executa o loop principal do agente para resolver uma tarefa dada.

        Este método constrói o contexto inicial e o prompt do sistema, depois entra em um loop de 
        raciocínio e ação (similar ao ReAct). Ele envia o histórico para o LLM, analisa a 
        resposta em busca de consultas SQL (se uma ferramenta estiver disponível), executa-as 
        e alimenta a saída de volta para o LLM até que uma resposta final seja alcançada ou 
        o número máximo de turnos seja excedido.

        Entradas:
            task_input (str): A tarefa ou pergunta do usuário para o agente.

        Saídas:
            str: A resposta de texto final do agente (ou uma mensagem de timeout).
        """
        print(f"\n--- Starting Agent: {self.name} ({self.role}) ---")
        
        # 1. Build Context
        schema_context = self.context_manager.get_schema_context(self.role)
        system_prompt = self._build_system_prompt(schema_context)
        
        self.history = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": task_input}
        ]

        # 2. Execution Loop (Reasoning + Tool Use)
        # We allow a few turns for self-healing
        max_turns = 20 
        
        for i in range(max_turns):
            response = self.llm.completion(self.history)
            
            if "ERROR:" in response and "OPENAI_API_KEY" in response:
                return response # Fail fast if no config

            print(f"Agent Thought: {response}")
            self.history.append({"role": "assistant", "content": response})

            # Check if agent wants to use Tool (simple heuristic: specific marker or sql code block)
            # For this custom implementation, we assume if the agent outputs SQL-like text, we run it.
            # OR we instruct the agent to output: QUERY: <sql>
            
            query = self._extract_query(response)
            
            if query and self.tool:
                tool_output = self.tool.run_query(query)
                self.history.append({"role": "user", "content": f"Tool Output: {tool_output}"})
                
                # Check if it was an error to encourage self-healing logic in next turn
                if "ERROR" in tool_output:
                    print("  -> Tool Error caught, retrying...")
                    continue # The LLM will see the error in history and retry
                else:
                    # If success, we might be done or need more analysis. 
                    # For simplicity, if we get data, we ask for final answer or just continue.
                    # As per instruction, the agent analyzes the data.
                    pass
            else:
                # If no query, assumption is the agent provided the final answer or analysis
                return response

        return "Agent timed out or failed to converge."

    def _build_system_prompt(self, schema_context):
        """
        Constrói o prompt do sistema para o agente com base em sua função e ferramentas disponíveis.

        Entradas:
            schema_context (str): A descrição do schema do banco de dados relevante para o domínio do agente.

        Saídas:
            str: Uma string formatada contendo as instruções do sistema e o contexto do schema.
        """
        base_prompt = f"Você é um agente especialista em {self.role} para o E-commerce Olist."
        base_prompt += "\nResponda e pense sempre em PORTUGUÊS."
        
        if self.tool:
            base_prompt += "\nVocê tem acesso a uma ferramenta SparkSQL. Para usá-la, forneça sua consulta SQL dentro de blocos ```sql ... ```."
            base_prompt += "\nSe sua consulta falhar, analise a mensagem de erro fornecida e corrija sua consulta."
            base_prompt += "\nIMPORTANTE: Use SEMPRE os nomes completos das tabelas conforme fornecido no contexto (ex: olist_dataset.olist_sales.nomedatabela)."
        else:
            base_prompt += "\nVocê não tem acesso ao banco de dados. Baseie-se apenas nos relatórios fornecidos."
            
        if schema_context:
            base_prompt += f"\n\nAqui está o Esquema do seu Domínio de Dados:\n{schema_context}"
            
        return base_prompt

    def _extract_query(self, text):
        """
        Extrai código SQL de blocos de código markdown no texto.

        Entradas:
            text (str): A resposta de texto do LLM contendo potenciais blocos SQL.

        Saídas:
            str ou None: A string da consulta SQL extraída se encontrada, caso contrário None.
        """
        if "```sql" in text:
            start = text.find("```sql") + 6
            end = text.find("```", start)
            return text[start:end].strip()
        return None
