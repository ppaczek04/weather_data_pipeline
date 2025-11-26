import os
import pypyodbc as odbc 

def bulk_insert(date_file, target_table):
    sql = f"""
        BULK INSERT {target_table}
        FROM '{date_file}'
        WITH
        (   
            FORMAT = 'CSV',
            FIRSTROW = 2, 
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\\n'
        )    
    """.strip()
    return sql

# --- Connection settings ---
SERVER_NAME = r"DESKTOP-3CO6VAE\SQLEXPRESS" # name of YOUR sql engine here
DATABASE_NAME = "Weather_DB" 
TARGET_TABLE = "bronze.weather_api_data"  

conn_str = f"""
    Driver={{SQL Server}};
    Server={SERVER_NAME};
    Database={DATABASE_NAME};
    Trusted_Connection=yes;
""".strip()

# we create connection to the database
conn = odbc.connect(conn_str)
print("Connected sccesfully to database:", conn)

# Iterate through CSV files and upload one by one 
data_file_folder = os.path.join(os.getcwd(), "data") 
data_files = [f for f in os.listdir(data_file_folder) if f.endswith(".csv")]

# we create cursor object to execute our SQL queries in our database
cursor = conn.cursor()

for data_file in data_files:
    full_path = os.path.join(data_file_folder, data_file)
    try:
        print(f"Start of file loading: {full_path}")
        cursor.execute(bulk_insert(full_path, TARGET_TABLE))
        conn.commit()  # push for one file at a time
        print(f"OK: {data_file}")
    except Exception as e:
        conn.rollback()  # in case of error we take back wrong data
        print(f"ERROR in file {data_file}: {e}")

#we close cursor and connection to our database after finishing
cursor.close()
conn.close()
print("All files loaded.")
