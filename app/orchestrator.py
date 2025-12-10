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
        print("\n=== STARTING AGENT PIPELINE ===\n")

        # Step 1: Logistics Agent (Diagnostic)
        print(">> Step 1: Logistics Diagnostic")
        logistics_task = "Analyze the delivery performance. Identify main causes for delays in the most recent orders."
        logistics_report = self.logistics_agent.run(logistics_task)
        print(f"\n[Logistics Report Generated]\n{logistics_report[:200]}...\n")

        # Step 2: Finance Agent (Impact)
        print(">> Step 2: Financial Impact Analysis")
        finance_task = f"""
        Based on the following logistics problems identified:
        '{logistics_report}'
        
        Calculate the financial volume involved in these delayed orders and potential revenue risk.
        To do this, you can query the payments and orders data.
        """
        finance_report = self.finance_agent.run(finance_task)
        print(f"\n[Finance Report Generated]\n{finance_report[:200]}...\n")

        # Step 3: COO (Strategy)
        print(">> Step 3: Strategic Synthesis (COO)")
        coo_task = f"""
        Review the technical findings below and propose a high-level strategic action plan in 'Business English' (or Corporate Portuguese).
        
        Logistics Findings:
        {logistics_report}
        
        Financial Impact:
        {finance_report}
        """
        final_plan = self.coo_agent.run(coo_task)
        
        print("\n=== FINAL STRATEGIC PLAN ===\n")
        print(final_plan)
        return final_plan

if __name__ == "__main__":
    orch = Orchestrator()
    orch.run_pipeline()
