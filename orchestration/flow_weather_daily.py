from datetime import datetime, timedelta
import os
import subprocess
import sys

from prefect import flow, task, get_run_logger

import pypyodbc as odbc  # or pyodbc, if you use pyodbc


# ====== CONFIGURATION ======
# Adjust if needed – you can also load these from environment variables

SERVER_NAME = r"DESKTOP-3CO6VAE\SQLEXPRESS"
DATABASE_NAME = "Weather_DB"


# ====== TASK 1: API DATA EXTRACTION ======
@task(retries=3, retry_delay_seconds=60)
def extract_yesterday_weather() -> str:
    logger = get_run_logger()

    yesterday = (datetime.now() - timedelta(days=1)).date()
    logger.info(f"Fetching weather data for: {yesterday}")

    import sys
    cmd = [sys.executable, "extract_weather_basic.py"]

    result = subprocess.run(
        cmd,
        cwd=os.path.dirname(os.path.dirname(__file__)),
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        logger.error("Failed to run extract_weather_basic.py")
        logger.error(f"STDOUT: {result.stdout}")
        logger.error(f"STDERR: {result.stderr}")
        raise RuntimeError("extract_weather_basic.py finished with an error")

    logger.info("Weather data extraction completed successfully.")
    logger.debug(f"STDOUT: {result.stdout}")
    return str(yesterday)


# ====== TASK 2: LOAD BRONZE ======
@task(retries=2, retry_delay_seconds=60)
def load_bronze() -> None:
    logger = get_run_logger()
    logger.info("Starting BRONZE layer loading (script_load_bronze.py)")

    import sys
    cmd = [sys.executable, "script_load_bronze.py"]

    result = subprocess.run(
        cmd,
        cwd=os.path.dirname(os.path.dirname(__file__)),
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        logger.error("Failed to run script_load_bronze.py")
        logger.error(f"STDOUT: {result.stdout}")
        logger.error(f"STDERR: {result.stderr}")
        raise RuntimeError("script_load_bronze.py finished with an error")

    logger.info("BRONZE layer loading completed successfully.")
    logger.debug(f"STDOUT: {result.stdout}")


# ====== TASK 3: LOAD SILVER (SQL PROCEDURE) ======
@task
def load_silver() -> None:
    """
    Connects to SQL Server and executes the stored procedure silver.load_silver.
    """
    logger = get_run_logger()
    logger.info("Starting SILVER layer loading (EXEC silver.load_silver)")

    conn_str = f"""
        Driver={{SQL Server}};
        Server={SERVER_NAME};
        Database={DATABASE_NAME};
        Trusted_Connection=yes;
    """.strip()

    try:
        conn = odbc.connect(conn_str)
    except Exception as e:
        logger.error(f"Failed to connect to SQL Server: {e}")
        raise

    try:
        cursor = conn.cursor()
        cursor.execute("EXEC silver.load_silver;")
        conn.commit()
        logger.info("Stored procedure silver.load_silver executed successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error while executing silver.load_silver: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
        logger.info("Database connection closed.")


# ====== MAIN PREFECT FLOW ======
@flow(name="weather_daily_flow")
def weather_daily_flow():
    """
    Main flow:
      1) Extracts weather data for the previous day,
      2) Loads CSVs into BRONZE,
      3) Runs SQL procedure to transform BRONZE → SILVER.
    """
    logger = get_run_logger()
    logger.info("=== Starting weather_daily_flow ===")

    processed_date = extract_yesterday_weather()
    logger.info(f"Data for {processed_date} fetched. Proceeding to BRONZE layer.")

    load_bronze()
    logger.info("BRONZE layer ready. Proceeding to SILVER layer.")

    load_silver()
    logger.info("SILVER layer ready. GOLD layer is always up-to-date via views.")

    logger.info("=== weather_daily_flow completed ===")


if __name__ == "__main__":
    weather_daily_flow()
