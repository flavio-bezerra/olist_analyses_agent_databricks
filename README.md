# Olist Agentic Data Mesh

Este projeto implementa uma arquitetura de Agentes de Dados utilizando Spark/Databricks e LLMs, seguindo a estratégia de Data Mesh.

## Estrutura

- **Data Mesh (Camada de Dados):** Cria 4 bancos de dados lógicos (`olist_sales`, `olist_logistics`, `olist_finance`, `olist_cx`) a partir dos CSVs.
- **SparkSQLTool (Camada de Conexão):** Ferramenta segura para execução de SQL com higienização, travas de volumetria e auto-cura.
- **ContextManager (Camada de Contexto):** Injeção dinâmica de schemas nos prompts dos agentes baseada no domínio.
- **Orquestrador (Camada de Agentes):** Execução sequencial: Logística -> Finanças -> COO.

## Personas dos Agentes

O sistema utiliza agentes com personalidades assertivas e papéis bem definidos para simular um ambiente corporativo real:

1. **Gerente de Logística (LogisticsAgent):**
   - **Perfil:** Analítico, direto e focado em eficiência operacional.
   - **Comportamento:** Não tolera atrasos sem explicação e busca incansavelmente gargalos na cadeia de suprimentos.
   - **Missão:** Diagnosticar problemas de entrega e performance de transportadoras baseado estritamente em dados.

2. **Diretor Financeiro (CFO) (FinanceAgent):**
   - **Perfil:** Conservador, pragmático, avesso a riscos e focado na proteção da margem.
   - **Comportamento:** Analisa cada centavo gasto e cobra resultados financeiros.
   - **Missão:** Calcular o prejuízo financeiro dos problemas operacionais e alertar sobre riscos de caixa.

3. **Chief Operating Officer (COO) (COOAgent):**
   - **Perfil:** Estratégico, visionário e orientado a solução.
   - **Comportamento:** Recebe inputs técnicos e decide "O que vamos fazer agora?".
   - **Missão:** Sintetizar relatórios em um plano de ação executivo de alto nível para o Board.

## Configuração

1. **Ambiente (Recomendado):**
   Crie um ambiente Conda estável usando o arquivo `environment.yml` fornecido:
   ```bash
   conda env create -f environment.yml
   conda activate olist_agent_env
   ```
   
   Ou instale via pip usando o arquivo `requirements.txt`:
   ```bash
   pip install -r requirements.txt
   ```
   *Nota: Java 8/11 é necessário para o PySpark.*

2. **Dados:**
   Crie uma pasta chamada `data` na raiz e adicione os seguintes arquivos do Dataset Olist:
   - `olist_orders_dataset.csv`
   - `olist_order_items_dataset.csv`
   - `olist_products_dataset.csv`
   - `olist_sellers_dataset.csv`
   - `olist_geolocation_dataset.csv`
   - `olist_customers_dataset.csv`
   - `olist_order_payments_dataset.csv`
   - `olist_order_reviews_dataset.csv`


## Execução
1. **Baixe os Dados (Uma vez):**
   Execute o notebook `notebooks/download_data.ipynb` para baixar automaticamente os dados do Kaggle e organizá-los na pasta `data/`.

2. **Prepare o Data Mesh (Uma vez):**
   Execute o notebook `notebooks/setup_data_mesh.ipynb` para carregar os CSVs e criar as tabelas no Spark.
   *Isso garante que o Agente tenha bancos de dados "olist_sales", "olist_logistics", etc. para consultar.*

3. **Execute o Agente:**
   Execute o script principal:
   ```bash
   python main.py
   ```
