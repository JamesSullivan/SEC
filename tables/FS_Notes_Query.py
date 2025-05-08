
# %% Cell 1
import os
import pandas as pd
import psycopg
from sqlalchemy import create_engine

def load_csvs_to_postgres(data_dir="../data/tables/", 
                         db_name="fin_report",
                         host="localhost", 
                         port="5432", 
                         user="puser", 
                         password="postgres"):
    """
    Loads all CSV files from a folder into a PostgreSQL database.
    
    Args:
        data_dir (str): The path to the folder containing the CSV files.
        db_name (str): The name of the PostgreSQL database to create or connect to.
        host (str): PostgreSQL server host.
        port (str): PostgreSQL server port.
        user (str): PostgreSQL username.
        password (str): PostgreSQL password.
    """
    
    # Connect to PostgreSQL server to create database if it doesn't exist
    with psycopg.connect(f"host={host} port={port} user={user} password={password}", autocommit=True) as conn:
        # Check if database exists, if not create it
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db_name,))
            exists = cursor.fetchone()
            if not exists:
                cursor.execute(f"CREATE DATABASE {db_name}")
                print(f"Created database '{db_name}'")
    
    # Create SQLAlchemy engine for efficient data loading
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
    
    # Connect to the specific database for operations
    conn = psycopg.connect(f"host={host} port={port} user={user} password={password} dbname={db_name}")
    cursor = conn.cursor()
    
    # Primary keys for different tables
    sec_pk = {
        'sub': 'adsh', 
        'tag': 'tag, version', 
        'ren': 'adsh, report', 
        'pre': 'adsh, report, line', 
        'cal': 'adsh, grp, arc', 
        'dim': 'dimhash'
    }
    
    # Date columns that need conversion
    sec_date_column = {
        'sub': ['changed', 'period', 'filed', 'floatdate'], 
        'num': ['ddate'], 
        'txt': ['ddate']
    }
    
    # List all files in the specified directory
    all_files = os.listdir(data_dir)
    csv_files = [f for f in all_files if f.endswith('.csv')]
    
    # Iterate through the CSV files and load them into PostgreSQL tables
    for file_name in csv_files:
        file_path = os.path.join(data_dir, file_name)
        table_name = os.path.splitext(file_name)[0]  # Use the filename (without extension) as the table name
        
        try:
            # Read CSV file with pandas
            df = pd.read_csv(file_path)
            
            # Convert data types for date columns if applicable
            if table_name in sec_date_column:
                for col in sec_date_column[table_name]:
                    if col in df.columns:
                        # Convert integer dates to PostgreSQL date format
                        df[col] = pd.to_datetime(df[col].astype(str), format='%Y%m%d', errors='coerce')
            
            # Write DataFrame to PostgreSQL
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            
            # Add primary key constraint if applicable
            if table_name in sec_pk:
                # Commit the current transaction
                conn.commit()
                
                # Add primary key constraint (PostgreSQL syntax)
                pk_columns = sec_pk[table_name]
                sql_pk = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({pk_columns});"
                print(sql_pk)
                cursor.execute(sql_pk)
                conn.commit()
            
            print(f"Loaded '{file_name}' into table '{table_name}'")
            
        except Exception as e:
            print(f"Error loading '{file_name}': {e}")
            # psycopg v3 automatically rolls back on exception in transactions
            print("Transaction rolled back automatically")
    
    cursor.close()
    conn.close()

# Example usage:
# Replace with your actual PostgreSQL database credentials and desired database name, host, and port.
# You need to have the database created in PostgreSQL beforehand.
name = 'alphabet-ms-nvidia'
load_csvs_to_postgres(data_dir=f"../data/tables_{name}",
                               db_name="fin_report",
                               host="localhost", # e.g., 'localhost' or an IP address
                               port="5432", # e.g., '5432' is the default PostgreSQL port
                               user="puser",
                               password=os.environ.get('AKASAKA_DB_PW', 'default_password')) 
# %%
