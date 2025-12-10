from app.data_engine import DataEngine
from app.tools import SparkSQLTool
from app.context_manager import ContextManager
from app.agents import Agent

class Orchestrator:
    def __init__(self):
        print("Initializing Orchestrator...")
        # 1. Initialize Data Layer
        self.data_engine = DataEngine()
        self.spark = self.data_engine.get_spark_session()
        # Data loading is now handled via 'setup_data_mesh.ipynb' or manual call to data_engine.ingest_data_mesh()
        # self.data_engine.ingest_data_mesh() # Uncomment if you want auto-ingest on every run

        # 2. Initialize Connection & Context Layers
        self.tool = SparkSQLTool(self.spark)
        self.context_manager = ContextManager(self.spark)

        # 3. Initialize Agents
        self.logistics_agent = Agent("LogisticsAgent", "logistics", self.context_manager, self.tool)
        self.finance_agent = Agent("FinanceAgent", "finance", self.context_manager, self.tool)
        self.coo_agent = Agent("COO", "coo", self.context_manager, tool=None) # COO has no SQL access

    def run_pipeline(self):
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
