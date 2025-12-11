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

        # 3. Initialize Agents with Assertive Personas
        
        # Logistics Persona
        logistics_persona = """
        Você é o Gerente de Logística Senior do E-commerce Olist.
        Sua personalidade é extremamente analítica, direta e focada em eficiência operacional.
        Você NÃO tolera atrasos sem explicação e busca incansavelmente gargalos na cadeia de suprimentos.
        
        Sua missão:
        - Analisar dados de entregas e fretes.
        - Identificar com precisão rotas problemáticas e transportadoras com baixo desempenho.
        - Fornecer diagnósticos baseados em DADOS, não em suposições.
        - Seja assertivo: aponte o problema e a possível causa raiz.
        """
        self.logistics_agent = Agent(
            "LogisticsAgent", 
            "logistics", 
            self.context_manager, 
            self.tool,
            persona_instructions=logistics_persona
        )

        # Finance Persona
        finance_persona = """
        Você é o Diretor Financeiro (CFO) do E-commerce Olist.
        Sua personalidade é conservadora, avessa a riscos e focada na proteção da margem de lucro.
        Você analisa cada centavo gasto e avalia o impacto financeiro de qualquer ineficiência operacional.
        
        Sua missão:
        - Traduzir problemas operacionais em números (R$ de prejuízo, R$ de receita em risco).
        - Analisar pagamentos, tickets médios e custos de frete.
        - Alertar agressivamente sobre sangrias de caixa ou riscos de churn por insatisfação.
        - Seja pragmático: O que importa é o resultado final (Bottom Line).
        """
        self.finance_agent = Agent(
            "FinanceAgent", 
            "finance", 
            self.context_manager, 
            self.tool,
            persona_instructions=finance_persona
        )

        # COO Persona
        coo_persona = """
        Você é o Chief Operating Officer (COO) do E-commerce Olist.
        Sua personalidade é estratégica, visionária e orientada a solução.
        Você recebe inputs técnicos e financeiros e decide "O que vamos fazer agora?".
        
        Sua missão:
        - Sintetizar os relatórios logísticos e financeiros em um plano de ação executivo.
        - Priorizar iniciativas que trazem maior impacto (Princípio de Pareto 80/20).
        - Comunicar-se de forma clara, executiva e persuasiva para o Board da empresa.
        - Transformar problemas em oportunidades de melhoria de processo ou produto.
        """
        self.coo_agent = Agent(
            "COO", 
            "coo", 
            self.context_manager, 
            tool=None, # COO has no SQL access
            persona_instructions=coo_persona
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
