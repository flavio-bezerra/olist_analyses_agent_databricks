"""
Orquestrador (O Chefe/Gerente)

Este arquivo é quem manda em tudo. Ele não faz o trabalho pesado, mas coordena quem faz o quê e quando.

Objetivo Didático:
Se os agentes são músicos, o Orchestrator é o regente. 
Ele:
1. Prepara o palco (inicializa o DataEngine e as ferramentas).
2. Chama o Agente de Logística para tocar sua parte (analisar entregas).
3. Pega o resultado da Logística e passa para o Agente de Finanças (calcular prejuízos).
4. Por fim, chama o COO para fazer o resumo final.
Sem ele, os agentes ficariam parados sem saber o que fazer.
"""
from app.data_engine import DataEngine
from app.tools import SparkSQLTool
from app.context_manager import ContextManager
from app.agents import Agent

class Orchestrator:
    def __init__(self):
        """
        Inicializa o Orquestrador.

        Configura todo o pipeline de agentes:
        1. inicializando a Camada de Dados (DataEngine),
        2. criando as Camadas de Conexão e Contexto (SparkSQLTool, ContextManager),
        3. instanciando os Agentes específicos (Logística, Finanças, COO) com suas respectivas funções e ferramentas.

        Entradas:
            Nenhuma

        Saídas:
            Nenhuma
        """
        print("Initializing Orchestrator...")
        # 1. Initialize Data Layer
        self.data_engine = DataEngine()
        self.spark = self.data_engine.get_spark_session()
        # Data loading is now handled via 'setup_data_mesh.ipynb' or manual call to data_engine.ingest_data_mesh()
        # self.data_engine.ingest_data_mesh() # Uncomment if you want auto-ingest on every run

        # 2. Initialize Connection & Context Layers
        self.tool = SparkSQLTool(self.spark)
        self.context_manager = ContextManager(self.spark)

        # Configuration: Model & Temperature Selection per Agent
        # Adjust these parameters based on performance/cost/creativity needs.
        # Options: 
        # - 'databricks-meta-llama-3-3-70b-instruct' (Balanced)
        # - 'databricks-meta-llama-3-1-405b-instruct' (Smartest/Slowest)
        # - 'databricks-llama-4-maverick' (Preview)
        # Temperature: 0.0-0.2 (Analytical) | 0.5-0.7 (Creative/Strategic)
        self.agent_config = {
            "logistics": {
                "model": "databricks-meta-llama-3-3-70b-instruct",
                "temperature": 0.1
            },
            "finance": {
                "model": "databricks-meta-llama-3-1-405b-instruct",
                "temperature": 0.1
            },
            "coo": {
                "model": "databricks-meta-llama-3-1-405b-instruct",
                "temperature": 0.5 # Higher temp for more natural & strategic writing
            }
        }

        # 3. Initialize Agents with Assertive Personas
        
        # Logistics Persona
        logistics_persona = """
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

⚠️ REGRAS ABSOLUTAS:
1. SÓ ANALISA PEDIDOS ENTREGUES:  
   ```sql
   WHERE order_status = 'delivered'
     AND order_delivered_customer_date IS NOT NULL
     AND order_estimated_delivery_date IS NOT NULL
	 
        """
        self.logistics_agent = Agent(
            "LogisticsAgent", 
            "logistics", 
            self.context_manager, 
            self.tool,
            persona_instructions=logistics_persona,
            model_name=self.agent_config["logistics"]["model"],
            temperature=self.agent_config["logistics"]["temperature"]
        )

        # Finance Persona
        finance_persona = """
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

⚠️ REGRAS ABSOLUTAS:
1. NUNCA TOQUE EM `olist_cx.order_reviews`: Tabela não estruturada, causa falhas. Ignorar completamente.
2. FOCO EM DINHEIRO REAL: Use apenas tabelas com dados transacionais:  
   - `olist_order_items` (price, freight_value, product_id)  
   - `olist_order_payments` (payment_value, installments)  
   - `olist_orders` (datas de aprovação e entrega)  
   - `marketing.cac_by_channel_q3_2025` (CAC por origem)
3. SEM ABSTRAÇÕES: Não fale de “engajamento” ou “fidelização”. Mostre perda de caixa.
4. UMA QUERY POR VEZ: Sem múltiplos comandos. Erro? Corrija sintaxe.
5. DATA REAL: Para pedidos não entregues, use `NOW()` como referência para cálculo de cycle time.

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
        """
        self.finance_agent = Agent(
            "FinanceAgent", 
            "finance", 
            self.context_manager, 
            self.tool,
            persona_instructions=finance_persona,
            model_name=self.agent_config["finance"]["model"],
            temperature=self.agent_config["finance"]["temperature"]
        )

        # COO Persona
        coo_persona = """
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

⚠️ REGRAS ABSOLUTAS:
1. NÃO DISCUTA SQL: Erros técnicos são problema dos diretores. Se dados forem inconsistentes, ordene auditoria interna.
2. DECISÃO COM CONSEQUÊNCIA: Toda escolha tem custo e benefício. Ex: Reduzir atraso pode aumentar frete — vale a pena?
3. OLHAR DE DONO: Você responde pelo CAC, LTV, NPS e EBITDA. Não fuja de trade-offs.
4. NADA DE BUROCRACIA: Suas ordens são diretas, com dono, meta e métrica.
5. DADOS INCONSISTENTES? TRATE COMO RISCO OPERACIONAL: Ordene reconciliação em 24h.

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
        """
        self.coo_agent = Agent(
            "COO", 
            "coo", 
            self.context_manager, 
            tool=None, # COO has no SQL access
            persona_instructions=coo_persona,
            model_name=self.agent_config["coo"]["model"],
            temperature=self.agent_config["coo"]["temperature"]
        )

    def run_pipeline(self):
        """
        Executa o pipeline sequencial de agentes (Logística -> Finanças -> COO).

        1. Agente de Logística: Diagnostica a performance de entrega e causas de atraso.
        2. Agente de Finanças: Calcula o impacto financeiro dos problemas identificados.
        3. Agente COO: Sintetiza as descobertas em um plano de ação estratégico.

        Entradas:
            Nenhuma

        Saídas:
            str: O plano de ação estratégico final gerado pelo Agente COO.
        """
        print("\n=== INICIANDO PIPELINE DE AGENTES ===\n")

        # Step 1: Logistics Agent (Diagnostic)
        print(">> Passo 1: Diagnóstico Logístico")
        logistics_task = "Analise a performance de entrega. Identifique as principais causas de atraso nos pedidos mais recentes."
        logistics_report = self.logistics_agent.run(logistics_task)
        print(f"\n[Relatório Logístico Gerado]\n{logistics_report[:200]}...\n")

        # Step 2: Finance Agent (Impact)
        print(">> Passo 2: Análise de Impacto Financeiro")
        finance_task = f"""
        Com base nos problemas logísticos identificados abaixo:
        '{logistics_report}'
        
        Calcule o volume financeiro envolvido nesses pedidos atrasados e o risco potencial de receita.
        Para isso, consulte os dados de pagamentos e pedidos.
        """
        finance_report = self.finance_agent.run(finance_task)
        print(f"\n[Relatório Financeiro Gerado]\n{finance_report[:200]}...\n")

        # Step 3: COO (Strategy)
        print(">> Passo 3: Síntese Estratégica (COO)")
        coo_task = f"""
        Revise os achados técnicos abaixo e proponha um plano de ação estratégico de alto nível em Português Corporativo.
        
        Achados Logísticos:
        {logistics_report}
        
        Impacto Financeiro:
        {finance_report}
        """
        final_plan = self.coo_agent.run(coo_task)
        
        print("\n=== PLANO ESTRATÉGICO FINAL ===\n")
        print(final_plan)
        return final_plan

if __name__ == "__main__":
    orch = Orchestrator()
    orch.run_pipeline()
