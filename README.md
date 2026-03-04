Airflow ETL Pipeline – Bronze, Silver, Gold Architecture

Project Overview

This project demonstrates a production-style ETL pipeline built using **Apache Airflow, Python, PostgreSQL, SQLAlchemy, and Docker**.

The pipeline implements a modern Bronze → Silver → Gold data architecture:

Bronze Layer – Raw employee data ingestion
Silver Layer – Cleaned and structured data
Gold Layer – Aggregated analytics-ready data

The system is fully containerized using Docker and orchestrated using Airflow.

---

Architecture


                +--------------------+
                |  Bronze Layer      |
                |  raw_employees     |
                +--------------------+
                          ↓
                +--------------------+
                |  Silver Layer      |
                |  employees         |
                +--------------------+
                          ↓
                +--------------------+
                |  Gold Layer        |
                |  employee_summary  |
                +--------------------+


Airflow DAG orchestrates:

1. bronze_to_silver
2. silver_to_gold

---

Tech Stack

* Python 3.12
* Apache Airflow 2.9
* PostgreSQL 15
* SQLAlchemy
* Docker & Docker Compose
* CeleryExecutor
* Redis

---

Project Structure


airflow/
│
├── dags/
│   ├── employee_etl_dag.py
│   └── etl/
│       ├── __init__.py
│       ├── bronzetosilver.py
│       └── silvertogold.py
│
├── logs/
├── plugins/
├── docker-compose.yml
├── requirements.txt
└── README.md


---

Data Flow

1. Bronze Layer

Stores raw employee records.

Table:
bronze.raw_employees

Silver Layer

Cleans and transforms raw data.

Table:
silver.cleaned_employees

Gold Layer
Aggregates business-level metrics per department.

Table:
gold.employee_summary

Example metrics:

* Total employees per department
* Total salary per department
* Average salary
* Load timestamp

How to Run the Project

Step 1 – Clone Repository
git clone https://github.com/yourusername/airflow-etl-pipeline.git
cd airflow-etl-pipeline

Step 2 – Start Docker Containers
docker compose up airflow-init
docker compose up -d

Step 3 – Access Airflow UI
Open in browser:
http://localhost:8081

Default login:

* Username: airflow
* Password: airflow

Step 4 – Trigger DAG

* Enable employee_etl_pipeline
* Click Trigger DAG
* Monitor task execution

Key Engineering Features

* Modular ETL design using Python functions
* SQLAlchemy engine management inside Airflow tasks
* Docker volume configuration for persistent PostgreSQL storage
* Container networking configuration (airflow-postgres service hostname)
* CeleryExecutor with Redis message broker
* Error handling and retry configuration
* Schema separation using Bronze, Silver, Gold architecture

Real-World Engineering Challenges Solved

* Fixed container networking issues (127.0.0.1 vs service hostname)
* Resolved Airflow DAG parsing errors
* Handled SQLAlchemy engine injection inside PythonOperator
* Managed Docker volume recursion issues
* Resolved schema mismatches between ETL logic and database
* Implemented persistent Postgres storage with Docker volumes


Why This Project Matters

This project demonstrates:

* End-to-end data engineering workflow
* Airflow orchestration
* Containerized data pipelines
* Data warehouse layering concepts
* SQL + Python integration
* Production-style debugging and troubleshooting

Future Enhancements

* Add data validation layer
* Implement Airflow Connections instead of hardcoded DB URLs
* Add logging & monitoring improvements
* Integrate CI/CD using GitHub Actions
* Add unit testing for ETL functions


Author

Gayathri T.P
Senior Software Engineer | Python | SQL | Airflow | PostgreSQL
GitHub: https://github.com/tpgayathritp


