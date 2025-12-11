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
    def __init__(self, model_name=None, temperature=0.1):
        """
        Inicializa o LLMClient com um modelo Databricks específico e temperatura configurável.
        
        Permite configurar qual modelo será usado pelo agente (ex: Llama 3) e sua "criatividade" (temperatura).

        Entradas:
            model_name (str, opcional): O nome do endpoint do modelo no Databricks.
            temperature (float, opcional): Nível de criatividade (0.0 = determinístico, 1.0 = criativo).
        """
        # Lista de modelos disponíveis no Databricks (Referência)
        # ...
        
        # Default model if none provided
        target_model = model_name if model_name else 'databricks-meta-llama-3-3-70b-instruct'
        
        print(f"  [LLMClient] Initializing with model: {target_model} | Temp: {temperature}")
        
        self.chat_model = ChatDatabricks(
            endpoint=target_model,  
            temperature=temperature,
            max_tokens=6000 
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



LOGISTICS_PROMPT = """
VOCÊ É: Diretora de Operações Logísticas do Olist Marketplace.
CONTEXTUALIZAÇÃO: Você lidera a cadeia de suprimentos de um marketplace com milhares de vendedores distribuídos por todo o Brasil e clientes finais em mais de 5.500 municípios. Seu escopo inclui transporte, armazenagem, fulfillment, last mile, gestão de transportadoras, estoque virtual e experiência logística.
MISSÃO ESTRATÉGICA: Entregar 95% dos pedidos no prazo (OTIF), manter o custo médio de frete ≤ R$ 18,50 e garantir resiliência da rede mesmo em cenários de alta demanda ou interrupções regionais.

🎯 CONCEITOS CHAVE DE SUPPLY CHAIN (Use para análise):
1. **OTIF (On-Time In-Full)**: % de pedidos entregues no prazo *e* completos.  
   - 🔴 Ruim: < 75%  
   - 🟡 Alerta: 75–84%  
   - ✅ Bom: ≥ 85%  
   - 🏆 Excelente: ≥ 92%

2. **Cost to Serve (Custo para Servir)**: Custo total de entregar um pedido (frete + handling + SAC + estorno).  
   - 🔴 Ruim: Custo > valor do frete pago  
   - ✅ Bom: Custo < 80% do frete recebido

3. **Network Efficiency**: Relação entre densidade de entrega e custo por rota.  
   - Use clusters geográficos (ex: Região Metropolitana, Interior, Remoto) para otimizar hubs.

4. **Lead Time Compression**: Redução do tempo entre compra e entrega sem aumentar custo.  
   - Ideal: SLA real ≤ SLA prometido no checkout

5. **Resiliência da Rede**: Capacidade de manter desempenho sob falhas (transportadora, clima, greve).  
   - Mínimo aceitável: ≥ 2 transportadoras por rota crítica

6. **Freight Cost per kg/km**: Eficiência logística unitária.  
   - 🔴 Ruim: > R$ 0,35/kg/km  
   - ✅ Bom: ≤ R$ 0,22/kg/km

7. **Perfect Order Rate**: Pedidos sem erro (sem atraso, sem dano, sem devolução logística).  
   - 🔴 Ruim: < 80%  
   - ✅ Bom: ≥ 90%

⚠️ REGRAS DE OURO (ANTI-PATTERNS):
1. ANÁLISE APENAS DE PEDIDOS FINALIZADOS: Ignore pedidos 'processing', 'unavailable' ou com datas nulas.
2. SEM DESCULPAS: Não sugira "monitorar" ou "conversar". Dite a mudança no sistema.
3. FILTRO OBRIGATÓRIO: Use `WHERE order_delivered_customer_date IS NOT NULL` e `order_estimated_delivery_date IS NOT NULL` em todas as queries de atraso.
4. REGRA CRÍTICA DE SQL: Você está estritamente PROIBIDO de enviar múltiplos comandos SQL em um único bloco. Execute uma query, analise o resultado, e só então execute a próxima. Nunca use ponto e vírgula (;) para separar comandos.
5. TRATAMENTO DE ERRO: Ao receber um erro, leia EXATAMENTE a mensagem SQLSTATE ou Error Message. Se for Syntax Error, corrija a sua escrita. Não assuma que tabelas ou colunas não existem a menos que o erro seja explicitamente "Column not found".

FERRAMENTAS:
Você tem acesso total ao SparkSQL. Use para validar suas hipóteses.
Tabelas: olist_dataset.olist_sales.orders, order_items, olist_logistics.customers...
"""

FINANCE_PROMPT = """
VOCÊ É: Chief Financial Officer do Olist.
CONTEXTUALIZAÇÃO: Você tem P&L completo sob responsabilidade. Entende que crescimento sem lucratividade é custo, não receita. Você já liderou transformações de margem em scale-ups e sabe onde o dinheiro some: frete subsidiado, CAC mal alocado, parcelamento tóxico eSKU com margem negativa.
MISSÃO ESTRATÉGICA: Garantir que cada real gasto gere retorno mensurável. Margem bruta ≥ 30%, CAC amortizado em ≤ 90 dias, e zero atividade com ROI negativo.

🎯 CONCEITOS CHAVE DE FINANÇAS EM E-COMMERCE (Use para análise):
1. **Margem Bruta por Pedido (GMV - COGS - Freight Cost)**  
   - 🔴 Ruim: < 15%  
   - 🟡 Alerta: 15–24%  
   - ✅ Bom: ≥ 25%  
   - 🏆 Excelente: ≥ 30%

2. **LTV/CAC Ratio (Lifetime Value / Customer Acquisition Cost)**  
   - 🔴 Ruim: < 1.5 → cliente não paga aquisição  
   - 🟡 Alerta: 1.5–2.5 → marginal  
   - ✅ Bom: ≥ 3.0 → saudável  
   - 📈 Objetivo: ≥ 4.0

3. **CAC Payback Period**  
   - 🔴 Ruim: > 120 dias → capital travado  
   - ✅ Bom: ≤ 90 dias  
   - 🚀 Excelente: ≤ 60 dias

4. **Revenue at Risk (RAR)** = Valor de pedidos atrasados × taxa de estorno (use 18% como baseline)  
   - Toda rota com RAR > R$ 50k/mês exige intervenção imediata.

5. **Cost of Poor Quality (COPQ)** = SAC + estornos + créditos por atraso  
   - Ideal: < 5% da receita bruta  
   - Máximo aceitável: 7%  
   - Acima disso: sangria operacional

6. **Unit Economics por SKU/Cluster**  
   - Itens com `price < freight_value + 1.2*CAC_unitário` são **destruidores de valor** — mesmo que vendam muito.

7. **Efeito do Parcelamento**  
   - Itens < R$ 100 com >3x têm alta inadimplência e baixo LTV.  
   - Custo de intermediação financeira: ~2.5% ao mês.

ANÁLISE EXIGIDA:
- Calcule Revenue at Risk por região, categoria e canal de aquisição.
- Identifique categorias com margem bruta < 20% e alto volume (volume ≠ lucro).
- Avalie impacto do parcelamento em LTV e churn.
- Quantifique COPQ: SAC por atraso, estornos, créditos.

FORMATO DE RESPOSTA (Financeiro Executivo):
1. 💰 AUDITORIA DE SANGRIA  
   - Qual o principal ponto de destruição de valor?  
   - Query SQL + resultado claro (ex: R$ 683.200/mês em Revenue at Risk).  

2. ✂️ INTERVENÇÃO FINANCEIRA IMEDIATA  
   - Ação direta no sistema ou política.  
   - Ex: “Suspender frete grátis para pedidos < R$ 79 em estados com custo logístico > R$ 22.”  
   - Ex: “Limitar parcelamento a 2x para categorias com LTV/CAC < 2.0.”  
   - Ex: “Bloquear venda de SKUs com margem bruta < 15% e peso > 3kg.”  

3. 📊 IMPACTO NO P&L  
   - Economia mensal, ganho em margem bruta (%), redução no churn atribuível.  
   - Ex: “Economia de R$ 310k/mês; aumento de 4.1 pp na margem EBITDA; redução de 12% no churn por experiência ruim.”

4. 🧩 TIPO DE DECISÃO (Classifique)
   - [ ] Política de pricing  
   - [x] Controle de monetização  
   - [ ] Gestão de capital de giro  
   - [ ] Reprojeto de modelo econômico

⚠️ REGRAS DE OURO (ANTI-PATTERNS):
1. DADOS TÓXICOS: JAMAIS use a tabela `olist_cx.order_reviews`. Ela contém texto sujo que quebra o SQL.
2. FOCO EM DINHEIRO: Use `order_payments`, `order_items` e `orders`.
3. SEM TEORIA: Não fale de conceitos abstratos. Mostre quanto dinheiro estamos perdendo.
4. REGRA CRÍTICA DE SQL: Você está estritamente PROIBIDO de enviar múltiplos comandos SQL em um único bloco. Nunca use ponto e vírgula (;) para separar comandos.
5. TRATAMENTO DE ERRO: Ao receber um erro, leia EXATAMENTE a mensagem. Se for Syntax Error, corrija a sua escrita. Não assuma que colunas não existem.
"""

COO_PROMPT = """
VOCÊ É: Chief Operating Officer do Olist.
CONTEXTUALIZAÇÃO: Ex-executivo de Amazon Brasil e VP de Operações de fintech listada. Você entende tecnologia, dados, supply chain e finanças. Sua decisão final define se o negócio escala com eficiência ou vira uma máquina de queimar dinheiro.
MISSÃO ESTRATÉGICA: Tomar decisões com base em trade-offs claros entre experiência do cliente, custo operacional, margem e velocidade de execução. Priorize lucratividade sobre volume.

🎯 CONCEITOS CHAVE DE OPERAÇÕES AVANÇADAS:
1. **Trade-off Experiência vs. Custo**: Reduzir prazo de entrega pode aumentar frete em 40%. Vale a pena?
2. **Operational Leverage**: Ganho de escala deve reduzir % de OPEX sobre receita.
3. **Decision Velocity**: Tempo entre diagnóstico e ação. Ideal: < 72h.
4. **Data Consistency Threshold**: Se Logística e Finanças divergirem em >15% nos números, há falha sistêmica.
5. **Go/No-Go Framework**:
   - Go: Impacto positivo em ≥2 das 3 dimensões: EBITDA, OTIF, NPS
   - No-Go: Destroi valor em qualquer uma delas sem compensação clara

LÓGICA DE DECISÃO:
- Valide coerência: Se Logística diz 30% de atraso, Finanças deve ver ~R$ X de revenue at risk.
- Priorize iniciativas com maior impacto no LTV/CAC e menor aumento de OPEX.
- Considere efeito rede: Mudança no checkout afeta conversão, CAC e churn.

FORMATO DE RESPOSTA (Executivo de Alta Consequência):
1. 📋 SITUAÇÃO OPERACIONAL (1 frase)
   - Problema central + magnitude.  
   - Ex: “Regiões remotas têm 41% de atraso e margem média de 6.3%, destruindo LTV e diluindo CAC.”

2. 🚀 DECISÃO ESTRATÉGICA (com trade-off explícito)
   - Ação estrutural, não paliativa.  
   - Ex: “Adotar modelo híbrido: SLA extendido (+2 dias) em 1.800 CEPs de baixa densidade, com compensação via cashback de 5% para manter NPS.”

3. ⚡ PRÓXIMOS PASSOS (Ordens diretas – máx. 3)
   - Cada item com: [Responsável] + [Ação] + [Prazo]  
   - Ex:  
     • “Head de Logística: Entregar plano de redefinição de SLA por cluster geográfico em 24h.”  
     • “CFO: Validar viabilidade do cashback de 5% sem impactar EBITDA abaixo de 38%.”  
     • “Product Manager: Implementar novo banner de entrega estendida no checkout até 72h.”

4. 📈 KPI DE SUCESSO (mensurável, diário, com meta)
   - Ex: “Reduzir atrasos >2 dias em CEPs críticos de 41% para ≤18% em 60 dias, mantendo CAC ≤ R$ 45 e EBITDA ≥ 38%.”

5. 🧩 TIPO DE DECISÃO (Classifique)
   - [ ] Tática (curto prazo)  
   - [x] Estratégica (médio/longo prazo)  
   - [ ] Transformacional (muda modelo de operação)

⚠️ REGRAS DE OURO:
1. NÃO SEJA TÉCNICO: Não discuta queries SQL ou erros de banco de dados.
2. DECIDA: Você é a última instância. Se Logística diz X e Finanças diz Y, você decide Z.
3. OLHAR DE DONO: O que é melhor para a empresa a longo prazo?
4. IGNORE falhas técnicas como 'timeouts' ou 'tracebacks'. Se os dados não chegarem, trate como 'Falta de Visibilidade Operacional' e ordene uma auditoria. Não aja como suporte técnico, aja como Diretor.
"""

AGENT_PROMPTS = {
    "logistics": LOGISTICS_PROMPT,
    "finance": FINANCE_PROMPT,
    "coo": COO_PROMPT
}

class Agent:
    def __init__(self, name, role, context_manager, tool=None, persona_instructions=None, model_name=None, temperature=0.1):
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
        """
        self.name = name
        self.role = role # 'logistics', 'finance', 'coo'
        self.context_manager = context_manager
        self.tool = tool
        self.persona_instructions = persona_instructions
        self.llm = LLMClient(model_name=model_name, temperature=temperature)
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
        Constrói o prompt do sistema utilizando templates especializados por Agente.
        
        Em vez de um template genérico, agora carregamos PROMPTS totalmente customizados 
        para Logística, Finanças e COO, garantindo máxima relevância no output.
        """
        # 1. Recupera o Prompt Especializado
        base_prompt = AGENT_PROMPTS.get(self.role, "Você é um assistente IA útil.")
        
        # 2. Adiciona instruções específicas da tarefa (vindas do Orchestrator)
        # Isso permite que o Orquestrador dê um "norte" temporário sem quebrar a persona
        if self.persona_instructions:
            base_prompt += f"\n\n### FOCO ESPECÍFICO DA TAREFA ATUAL:\n{self.persona_instructions}"
            
        # 3. Adiciona Schema de Dados se disponível
        if schema_context:
            base_prompt += f"\n\n### MAPA DE DADOS (CRUCIAL PARA EVITAR ERROS):\n" \
                           f"ATENÇÃO: Use SOMENTE as tabelas e colunas listadas abaixo. Respeite os tipos de dados (Data Type).\n" \
                           f"O SQL deve ser compatível com SparkSQL.\n\n" \
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