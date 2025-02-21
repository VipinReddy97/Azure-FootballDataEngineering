# **Football Data Engineering**

This Python-based project extracts football-related data from Wikipedia using **Apache Airflow**, processes it for cleaning and transformation, and then stores it in **Azure Data Lake** for further analysis.

---

## **Table of Contents**
1. [System Architecture](#system-architecture)  
2. [Prerequisites](#prerequisites)  
3. [Setup Instructions](#setup-instructions)  
4. [Executing with Docker](#executing-with-docker)  
5. [Workflow Overview](#workflow-overview)  

---

## **System Architecture**

The workflow integrates multiple components to ensure smooth data processing:  
- **Wikipedia** – Source of raw data  
- **Apache Airflow** – Manages workflow orchestration  
- **PostgreSQL** – Stores intermediate data  
- **Azure Data Factory** – Transfers data efficiently  
- **Azure Data Lake Gen2** – Serves as the primary data storage  
- **Databricks** – Facilitates data transformation and analytics  
- **Visualization Tools** – Data insights are derived using **Tableau, Power BI, and Looker Studio**  

![System Architecture](assets/architecture-diagram.png)

---

## **Prerequisites**
Ensure the following dependencies are installed before running the project:  
- Python 3.9 or later  
- Docker  
- PostgreSQL  
- Apache Airflow 2.6 or newer  

---

## **Setup Instructions**
To get started, follow these steps:

1. **Clone the repository:**
   ```bash
   git clone https://github.com/airscholar/FootballDataEngineering.git
   ```
   
2. **Install required dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Executing with Docker**
   ```bash
   docker compose up -d
   ```
Trigger the DAG within the Apache Airflow UI.


## **Workflow Overview**
1. **Data Extraction with Apache Airflow** – Orchestrates the extraction of raw football data from Wikipedia using custom Airflow DAGs.
2. **Data Cleaning & Transformation** – Airflow DAGs handle preprocessing, cleansing, and structuring the data for consistency and usability.
3. **Data Storage in Azure Data Lake** – The transformed data is written to Azure Data Lake Gen2, ensuring scalable and secure storage.
4. **Data Processing with Azure Synapse** – The stored data is ingested into Azure Synapse Analytics, enabling high-performance querying and large-scale analytics.
5. **Visualization with Tableau** – Tableau is connected to Azure Synapse Analytics to generate interactive dashboards for data-driven insights.
     
   
