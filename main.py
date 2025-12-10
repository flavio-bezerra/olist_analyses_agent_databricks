from app.orchestrator import Orchestrator
import os
import sys

def main():
    # Check for data directory
    if not os.path.exists("data"):
        print("WARNING: 'data/' directory not found. Creating it.")
        os.makedirs("data")
        print("Please place the Olist CSV files in the 'data/' folder so the agents have data to work with.")
        
    try:
        orch = Orchestrator()
        orch.run_pipeline()
    except Exception as e:
        print(f"An error occurred during execution: {e}")

if __name__ == "__main__":
    main()
