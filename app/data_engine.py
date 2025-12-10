"""
Motor de Dados (A Fundação)

Este módulo é responsável por "construir o prédio" onde os agentes vão trabalhar. 
Ele lida com a infraestrutura de dados bruta.

Objetivo Didático:
1. Inicializar o Spark: Liga o motor de processamento de dados.
2. Ingestão de Dados: Lê os arquivos CSV soltos (raw data) e os organiza em "bancos de dados" lógicos (Data Mesh).
   É como pegar papéis espalhados numa mesa e organizá-los em pastas etiquetadas (Vendas, Logística, Finanças) 
   para que os agentes possam encontrar facilmente.
"""
from pyspark.sql import SparkSession
import os

class DataEngine:
    def __init__(self, data_path="data/"):
        """
        Inicializa o DataEngine.

        Define o caminho onde os arquivos de dados CSV estão localizados. Não inicializa 
        imediatamente a sessão Spark para evitar potenciais conflitos em ambientes de notebook.

        Entradas:
            data_path (str, opcional): Caminho relativo ou absoluto para a pasta de dados. 
                                       Padrão é "data/".

        Saídas:
            Nenhuma
        """
        self.data_path = data_path
        # Do not initialize spark here to avoid conflict in Databricks
        self.spark = None

    def initialize_session(self):
        """
        Obtém a sessão Spark existente ou cria uma nova, se necessário.
        
        Tenta recuperar uma sessão ativa (comum em ambientes Databricks ou 
        Jupyter) para reutilizar recursos. Se nenhuma existir, constrói uma 
        nova sessão Spark local.

        Entradas:
            Nenhuma

        Saídas:
            SparkSession: O objeto de sessão Spark ativo.
        """
        if self.spark is None:
            # Try to get existing session (Databricks default)
            self.spark = SparkSession.getActiveSession()
            
            # Fallback for local testing ONLY if no session exists
            if self.spark is None:
                self.spark = SparkSession.builder \
                    .appName("OlistDataMesh") \
                    .getOrCreate()
        
        return self.spark

    def get_spark_session(self):
        """
        Carregador preguiçoso (lazy loader) para a sessão Spark.

        Garante que a sessão seja inicializada apenas quando solicitada. Este método é 
        seguro para ser chamado repetidamente, pois retornará o `self.spark` existente se já estiver definido.

        Entradas:
            Nenhuma

        Saídas:
            SparkSession: A sessão Spark ativa.
        """
        # Lazy load ensures we don't crash on import/init
        if self.spark is None:
            return self.initialize_session()
        return self.spark

    def ingest_data_mesh(self):
        """
        Cria os bancos de dados lógicos (Domínios de Data Mesh) e ingere dados de CSVs.
        
        Este método analisa a estrutura de domínio definida, cria os Catálogos/Schemas/Bancos de Dados 
        necessários no Spark e carrega o conteúdo CSV em tabelas gerenciadas. 
        Lida com falhas na criação de catálogos (usando fallback para o padrão) e erros específicos 
        de carregamento de arquivos de forma graciosa.

        Entradas:
            Nenhuma (usa self.data_path e mapeamento interno DOMAINS)

        Saídas:
            Nenhuma (Imprime mensagens de status no stdout)
        """
        domains = {
            "olist_sales": ["olist_orders_dataset.csv", "olist_order_items_dataset.csv", "olist_products_dataset.csv"],
            "olist_logistics": ["olist_sellers_dataset.csv", "olist_geolocation_dataset.csv", "olist_customers_dataset.csv"],
            "olist_finance": ["olist_order_payments_dataset.csv"],
            "olist_cx": ["olist_order_reviews_dataset.csv"]
        }

        # Try to create catalog
        try:
             self.spark.sql("CREATE CATALOG IF NOT EXISTS olist_dataset")
             catalog_prefix = "olist_dataset."
        except:
             print("Warning: Could not create catalog. Using default.")
             catalog_prefix = ""

        for domain, files in domains.items():
            db_name = f"{catalog_prefix}{domain}"
            print(f"Creating Domain: {db_name}")
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            
            for file in files:
                table_name = file.replace("olist_", "").replace("_dataset.csv", "")
                file_path = os.path.join(self.data_path, file)
                
                # Check if file exists to avoid errors during empty runs
                if os.path.exists(file_path):
                    target_table = f"{db_name}.{table_name}"
                    print(f"  Loading {table_name} from {file_path} into {target_table}")
                    try:
                        df = self.spark.read.csv(file_path, header=True, inferSchema=True)
                        # Save as table in the specific database
                        df.write.mode("overwrite").saveAsTable(target_table)
                    except Exception as e:
                        print(f"  Error loading {table_name}: {e}")
                else:
                    print(f"  Warning: File {file_path} not found. Skipping table.")
