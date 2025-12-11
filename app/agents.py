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
    def __init__(self, model_name=None, temperature=0.1, max_tokens=6000):
        """
        Inicializa o LLMClient com um modelo Databricks específico, temperatura e limite de tokens.
        
        Permite configurar qual modelo será usado, sua criatividade e o tamanho máximo da resposta.

        Entradas:
            model_name (str, opcional): O nome do endpoint do modelo no Databricks.
            temperature (float, opcional): Nível de criatividade (0.0 = determinístico, 1.0 = criativo).
            max_tokens (int, opcional): Número máximo de tokens na resposta (padrão: 6000).
        """
        # Lista de modelos disponíveis no Databricks (Referência)
        # ...
        
        # Default model if none provided
        target_model = model_name if model_name else 'databricks-meta-llama-3-3-70b-instruct'
        
        print(f"  [LLMClient] Initializing with model: {target_model} | Temp: {temperature} | MaxTokens: {max_tokens}")
        
        self.chat_model = ChatDatabricks(
            endpoint=target_model,  
            temperature=temperature,
            max_tokens=max_tokens 
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

# ... (Prompts definitions are here, skipping for brevity in this replace block if not touched, 
# but I need to reach Agent class which is further down. 
# Since I cannot skip lines in replace_file_content effectively if they are between edits without making a huge block, 
# I will just replace LLMClient and then make another call for Agent or include Agent if it's close enough.
# Agent is at line 167. This replacement block is seemingly ending at line 88? 
# Wait, I can do two chunks or one large chunk. The prompts are in between.
# Let's do two chunks with multi_replace_file_content.



# Prompts removidos. A definição de persona agora é injetada exclusivamente pelo Orchestrator.


class Agent:
    def __init__(self, name, role, context_manager, tool=None, persona_instructions=None, model_name=None, temperature=0.1, max_tokens=6000):
        """
        Inicializa um Agente com uma função, contexto, ferramentas e configurações de LLM.

        Entradas:
            name (str): O nome do agente (ex: "LogisticsAgent").
            role (str): A função do agente ('logistics', 'finance', 'coo').
            context_manager (ContextManager): Um gerenciador para recuperar informações de schema e contexto.
            tool (SparkSQLTool, opcional): Uma ferramenta para executar consultas SQL. Padrão é None.
            persona_instructions (str, opcional): Instruções detalhadas sobre a persona e comportamento do agente.
            model_name (str, opcional): Nome do modelo LLM a ser usado por este agente.
            temperature (float, opcional): Temperatura do LLM (criatividade).
            max_tokens (int, opcional): Limite máximo de tokens de saída.
        """
        self.name = name
        self.role = role # 'logistics', 'finance', 'coo'
        self.context_manager = context_manager
        self.tool = tool
        self.persona_instructions = persona_instructions
        self.llm = LLMClient(model_name=model_name, temperature=temperature, max_tokens=max_tokens)
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
        print(f"\\n--- Starting Agent: {self.name} ({self.role}) ---")
        
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
        Constrói o prompt do sistema utilizando templates injetados pelo Orchestrator.
        """
        base_prompt = self.persona_instructions if self.persona_instructions else "Você é um assistente IA analítico útil."
            
        if schema_context:
            # ADICIONADO: Instrução explícita sobre Qualified Names
            base_prompt += f"\n\n### REGRAS DE SQL (SEGURANÇA):\n" \
                           f"1. Use SEMPRE o nome totalmente qualificado das tabelas (ex: olist_dataset.olist_sales.orders).\n" \
                           f"2. O comando 'USE database' não é permitido. Referencie o schema direto na query.\n" \
                           f"3. Verifique os nomes das colunas abaixo antes de criar filtros.\n\n" \
                           f"### MAPA DE DADOS:\n" \
                           f"{schema_context}"
            
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