# Automated CSV ETL Pipeline with Apache Airflow, Python, Docker & PostgreSQL

## 📌 Project Overview

This project implements a reusable ETL (Extract, Transform, Load) pipeline that automates CSV data processing using **Python, Apache Airflow, Docker, and PostgreSQL**.

The pipeline discovers CSV files, validates data quality, removes duplicate records, rejects invalid data, generates quality reports, and loads cleaned records into PostgreSQL. Apache Airflow orchestrates the workflow through a scheduled, task-based DAG.

## 🎯 Objectives

- Automate CSV data ingestion and processing.
- Implement reusable data-cleaning and validation rules.
- Identify and reject duplicate or invalid records.
- Load validated datasets into PostgreSQL.
- Orchestrate ETL tasks using Apache Airflow.
- Run the project in a containerized Docker environment.

## 🛠️ Technologies Used

| Technology              | Purpose                                        |
| ----------------------- | ---------------------------------------------- |
| Python                  | ETL logic and data validation                  |
| Apache Airflow          | Workflow orchestration and scheduling          |
| PostgreSQL              | Relational database for cleaned data           |
| Docker & Docker Compose | Containerized application environment          |
| SQL                     | Table creation and data verification           |
| CSV & JSON              | Input datasets and configurable cleaning rules |
| Git & GitHub            | Version control and project documentation      |

## 🏗️ Architecture

```text
        CSV Input Files
               |
               v
     Apache Airflow DAG
               |
               v
     Discover CSV Files
               |
               v
      Data Validation
      and Transformation
               |
         +-----+-----+
         |           |
         v           v
   Cleaned Data   Rejected Data
         |           |
         v           v
   Quality Report   Rejection
         |           Records
         v
   PostgreSQL Tables
         |
         v
   Data Verification
```

## ⚙️ ETL Workflow

1. **Extract:** Discover CSV files in the input directory.
2. **Validate:** Check required columns, missing values, numeric fields, and invalid values.
3. **Transform:** Normalize data, remove duplicates, and apply configurable cleaning rules.
4. **Separate records:** Store valid records in cleaned CSV files and invalid records in rejected files with rejection reasons.
5. **Generate reports:** Record processing outcomes and data quality information.
6. **Load:** Insert cleaned sales and employee records into PostgreSQL.
7. **Orchestrate:** Execute the ETL tasks through an Apache Airflow DAG.

## 📂 Project Structure

```text
airflow-csv-etl-pipeline/
├── README.md
├── .gitignore
├── docker-compose.yaml
└── dags/
    ├── reusable_etl_pipeline.py
    ├── config/
    │   └── cleaning_rules.json
    ├── scripts/
    │   ├── etl_engine.py
    │   └── load_to_postgres.py
    └── data/
        └── input/
            ├── sales.csv
            └── employees.csv
```

Generated files and local runtime data may also exist during execution but should generally not be committed to GitHub.

## 📊 Datasets

### 1. Sales Dataset

Contains sales order information such as:

- Order ID
- Product
- Quantity
- Price

Validation examples include duplicate order IDs, missing quantities, and non-positive quantities or prices.

### 2. Employees Dataset

Contains employee information such as:

- Employee ID
- Name
- Department
- Salary

Validation examples include duplicate employee IDs, missing departments, and non-positive salaries.

## 🗄️ PostgreSQL Tables

The pipeline loads cleaned records into:

- `sales_cleaned`
- `employees_cleaned`

Both tables include a `processed_at` timestamp to record when a row was processed or inserted.

## 🔄 Apache Airflow DAG

**DAG ID:** `reusable_csv_etl_pipeline`

The DAG contains two main tasks:

1. `discover_clean_validate_csv_files`
2. `load_cleaned_data_to_postgres`

The cleaning task runs before the PostgreSQL loading task. The DAG is configured for daily scheduling, with retries enabled for task failures.

## 🚀 How to Run the Project

### Prerequisites

- Docker Desktop
- Git
- Python knowledge for modifying the ETL scripts

### Step 1: Clone the repository

```bash
git clone https://github.com/<your-username>/airflow-csv-etl-pipeline.git
cd airflow-csv-etl-pipeline
```

Replace `<your-username>` with your GitHub username.

### Step 2: Configure the environment

Review the Docker Compose configuration and set the required environment variables securely. Do not commit `.env` files or credentials.

### Step 3: Start the services

```bash
docker compose up -d
```

Use the repository's documented Airflow initialization procedure if required.

### Step 4: Open Airflow

Visit:

```text
http://localhost:8080
```

Sign in using the credentials configured for your local Airflow environment.

### Step 5: Run the pipeline

1. Find `reusable_csv_etl_pipeline` in the Airflow UI.
2. Unpause the DAG if necessary.
3. Trigger a run.
4. Open the task logs to inspect processing results.

Ensure the sample CSV files are present in the input directory before triggering the pipeline. Processed input files may be moved to an archive directory.

### Step 6: Verify PostgreSQL data

Connect to the project's PostgreSQL database and run:

```sql
SELECT * FROM sales_cleaned;

SELECT * FROM employees_cleaned;
```

## ✅ Key Features

- Reusable CSV ETL engine.
- Configurable validation rules using JSON.
- Duplicate detection and invalid-record rejection.
- Data quality reporting.
- PostgreSQL integration.
- Workflow scheduling, task dependencies, and retries with Airflow.
- Docker-based local development environment.

## 🔐 Security and Data Handling

- Keep credentials and environment files out of version control.
- Use `.gitignore` to exclude generated files, logs, caches, and local database artifacts.
- Use sample datasets without confidential information.
- Configure production credentials using a secure secrets-management approach.

## 🔮 Future Improvements

- Make database loading idempotent to prevent duplicate rows on reruns.
- Add automated unit tests for cleaning and validation logic.
- Add structured logging and stronger error handling.
- Introduce incremental loading and upsert logic.
- Add data quality metrics and monitoring dashboards.
- Deploy the pipeline to a cloud platform.

## 👨‍💻 Author

**SRIPADA PAVAN KUMAR**

GitHub :https://github.com/MrSripadapavan75

---

*This project demonstrates practical skills in ETL development, data quality validation, workflow orchestration, SQL, PostgreSQL, Docker, and Python-based data engineering.*
