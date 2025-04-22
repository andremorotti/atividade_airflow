# Apache Airflow & Spark
**EN:** This repository is intended for orchestrating Spark applications using the Apache Airflow tool.  
**PT:** Este repositório tem como objetivo orquestrar aplicações Spark utilizando a ferramenta Apache Airflow.

---

## Table of Contents / Índice

1. [Technologies / Tecnologias](#1-technologies--tecnologias)  
2. [Install and Run / Instalação e Execução](#2-install-and-run--instalação-e-execução)  
3. [Apache Airflow Webserver / Servidor Web do Apache Airflow](#3-apache-airflow-webserver--servidor-web-do-apache-airflow)  
4. [Databricks Lakehouse Architecture / Arquitetura Lakehouse com Databricks](#4-databricks-lakehouse-architecture--arquitetura-lakehouse-com-databricks)

---

## 1. Technologies / Tecnologias

**EN:** A list of technologies used within the project:  
**PT:** Lista de tecnologias utilizadas neste projeto:

- [Python](https://www.python.org): Version 3.12  
- [PySpark](https://spark.apache.org/docs/latest/api/python/index.html): Version 3.5.5  
- [Apache Airflow](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html): Version 2.10.5  

---

## 2. Install and Run / Instalação e Execução

### Clone the repository / Clone o repositório

```bash
git clone https://github.com/andremorotti/atividade_airflow.git  # insira a URL do seu repositório
```

### Windows

```bash
# EN: Create and activate virtual environment
# PT: Crie e ative um ambiente virtual
python -m venv venv
venv\Scripts\activate

# EN: Install dependencies
# PT: Instale as dependências
pip install -r requirements.txt
```

### MacOS & Linux

```bash
# EN: Create and activate virtual environment
# PT: Crie e ative um ambiente virtual
python3 -m venv venv  # ou virtualenv venv
source venv/bin/activate

# EN: Install dependencies
# PT: Instale as dependências
pip install -r requirements.txt
```

---

### Config and Run Apache Airflow / Configurar e Executar o Apache Airflow

**EN:** Ensure that Apache Airflow is installed on your machine.  
**PT:** Certifique-se de que o Apache Airflow está instalado na sua máquina.

#### 1. Initialize Airflow / Inicialize o Airflow

```bash
airflow db init
```

#### 2. (Optional) Create user / (Opcional) Criar um usuário

```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

#### 3. (Alternative) Run standalone / (Alternativa) Rodar em modo standalone

```bash
airflow standalone
```
> **EN:** This will start webserver and scheduler and create an admin user.  
> **PT:** Isso iniciará o webserver e o scheduler e criará um usuário administrador automaticamente.

#### 4. Access Airflow UI / Acesse a interface do Airflow

```
http://localhost:8080
```

#### DAG Path Configuration / Configuração do Caminho do DAG

```bash
cd airflow
nano airflow.cfg
```

**EN:** Change `dags_folder` to your local DAG path.  
**PT:** Altere `dags_folder` para o caminho local do seu DAG.

```ini
# dags_folder = /home/youruser/airflow/dags
dags_folder = /home/your/path/to/this/repo/dags #/home/seuusuario/caminho/para/o/repositorio/dags
```

---

## 3. Apache Airflow Webserver / Servidor Web do Apache Airflow

**EN:** Once the webserver is running, open `http://127.0.0.1:8080` in your browser.  
**PT:** Com o servidor rodando, acesse `http://127.0.0.1:8080` no navegador.

1. **EN:** Change Spark connection host  
   **PT:** Altere o host da conexão Spark  
   **UI path:** Admin > Connections > `spark_default` > Change `Host` from `"yarn"` to `"local"`

2. **EN:** Trigger DAG  
   **PT:** Dispare a DAG  
   Search for your DAG by name in the "Search Dags" field and click "Trigger DAG".

**EN:** You can verify execution in the `bronze`, `silver`, and `gold` folders — each will have a `parquet` subfolder if the job succeeded.  
**PT:** Verifique a execução nas pastas `bronze`, `silver` e `gold`, onde uma subpasta chamada `parquet` será criada em caso de sucesso.

---

## 4. Databricks Lakehouse Architecture / Arquitetura Lakehouse com Databricks

**EN:**  
The Medallion Architecture is a data design pattern used to logically organize data in a lakehouse. It aims to progressively enhance data quality across three layers:  
- **Bronze:** raw data  
- **Silver:** cleaned and transformed data  
- **Gold:** analytics-ready data  

**PT:**  
A Arquitetura Medalhão é um padrão de design de dados usado para organizar logicamente os dados em um lakehouse. Seu objetivo é aprimorar progressivamente a qualidade dos dados em três camadas:  
- **Bronze:** dados brutos  
- **Silver:** dados limpos e transformados  
- **Gold:** dados prontos para análise  
