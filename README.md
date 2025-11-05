# 🌤️ Weather Data Pipeline (SQL Server + dbt + Prefect)

## 🧾 1. Description
This project demonstrates an end-to-end **data engineering pipeline** built around a modern ELT architecture.  
The goal is to **collect live weather data** from a public API, load it into a **SQL Server database**, transform it through **dbt models** following the **Medallion architecture (Bronze, Silver, Gold)**, and finally visualize the results in **Power BI**.  
The workflow is orchestrated using **Prefect**, allowing automatic and scheduled data updates.

---

## 🛠️ 2. Technologies
| Layer | Tools & Libraries |
|-------|--------------------|
| **Data Ingestion** | Python (`requests`, `pandas`, `sqlalchemy`) |
| **Database** | Microsoft SQL Server 2021 |
| **Transformation & Testing** | `dbt Core` + `dbt-sqlserver` adapter + built-in `dbt tests` |
| **Orchestration** | Prefect |
| **Visualization** | Power BI |
| **Environment** | Windows 11, Visual Studio Code, virtual environment (`venv`) |

---

## 🧱 3. Planned Data Architecture
        +---------------------------+
        |      Weather API          |
        +-------------+-------------+
                    |
                    v
        [Python] -> Bronze Layer 
                (Raw Data)
                    |
                    v
            [dbt] -> Silver Layer 
            (Cleaned & Validated)
                    |
                    v
            [dbt] -> Gold Layer 
            (Aggregated & Business Views)
                    |
                    v
            [Power BI Dashboard]

- **Bronze Layer** – stores raw API data as-is.  
- **Silver Layer** – cleans, validates, and standardizes the data.  
- **Gold Layer** – aggregates data for reporting (daily averages, trends, alerts).  
- **Prefect** orchestrates each step of the process (Extract → Transform → Test → Report).




## 👨‍💻 4. About Me
I’m an aspiring **Data Engineer** with a strong interest in building automated, scalable data pipelines. 
This project is part of my personal portfolio to demonstrate practical data engineering skills.

