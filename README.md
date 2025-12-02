# 🌤️ Weather Data Pipeline (SQL Server + Prefect ETL Orchiestration tool)

## 1. Project Description

This project implements an end-to-end **data pipeline automated with an orchestration tool** that collects hourly weather measurements from the **Open-Meteo API**, processes them through a layered data architecture, and stores them in a SQL Server database. The workflow is fully orchestrated using **Prefect**, enabling daily automation without manual involvement.

The pipeline performs the following steps:

* Fetches weather data from the API and saves it as timestamped CSV files.
* Loads the raw CSVs into the **Bronze layer** using SQL Server BULK INSERT.
* Transforms and cleans the data in the **Silver layer** through a SQL stored procedure.
* Exposes analytics-ready **Gold views** for reporting and visualization.

This project demonstrates key Data Engineering concepts such as API ingestion, layered data modeling (Bronze/Silver/Gold), SQL transformations, workflow orchestration, and automated scheduling.


## 2. Technological Stack

This project is built using the following technologies:

* **Python 3.12** – main programming language for extraction and orchestration scripts.
* **Pandas** – processing API responses and generating CSV files.
* **Microsoft SQL Server** – database engine used for Bronze, Silver, and Gold layers.
* **T-SQL** – stored procedures, transformations, and view definitions.
* **PyODBC / PypyODBC** – database connectors for loading data into SQL Server.
* **Prefect 3** – workflow orchestration, scheduling, and automation.
* **Requests from Open-Meteo API** – weather data source.
* **Virtual Environment (`weather_env`)** – all dependencies installed and isolated inside the project environment.

This stack provides a clean, modular, and production-like setup suitable for automated daily ETL/ELT pipelines.

## 3. Data Architecture & Pipeline Flow

<div style="color:#ff9124; text-align:center;">

  <p>
    This project shows a classical Data Warehouse implemented according to the principles 
    of the Medallion architecture.
  </p>

  <p>
    Every day at 20:00 (Europe/Warsaw) the Prefect scheduler triggers the 
    <code>weather_daily_flow</code> pipeline. The workflow consists of the following stages:
  </p>

</div>

![data_arch_scheme](resources/dwh_architecture.png "Tutaj wpisz tekst po najechaniu")

#### 1. API Extraction (Previous Day’s Weather Data)

A Python script sends a request to the Open-Meteo API for all configured U.S. state capitals.
The API returns hourly observations for the previous calendar day.
The data is saved locally as a timestamped CSV file inside the data/ directory.

#### 2. Bronze Layer Loading (One File at a Time)

A separate Python ingestion script iterates through all CSV files in the data/ folder.
Each file is inserted into the Bronze table using SQL Server BULK INSERT.
CSVs are processed independently, meaning:  
**if a single file fails to load, only that file is rolled back,
all other valid files are still loaded successfully**.    
This ensures a modular, fault-tolerant ingestion process.

#### 3. Silver Layer Transformation (Stored Procedure)

After the Bronze load completes, Prefect executes a SQL stored procedure (silver.load_silver).
The procedure cleans and transforms the Bronze data:
converts timestamp strings,
extracts the date and hour components,
standardizes city/state fields,
applies indexing for performance.
The transformed output is inserted into the Silver table.

#### 4. Gold Layer (Views for Analytics)

The Gold layer is implemented as SQL views created once during setup.
These views include daily aggregations and the most recent forecast snapshots.
Because they depend directly on Silver, they always remain up-to-date after each pipeline run.
This architecture cleanly separates raw, processed, and analytical data while enabling a fully automated daily refresh powered by Prefect.


