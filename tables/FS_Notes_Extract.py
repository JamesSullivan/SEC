# %% Cell 1
# Helper functions for extracting data from SEC files
import os
import pandas as pd
import sys
import zipfile

from IPython.display import display, HTML
from typing import List, Set, Dict, Tuple, Optional

# ciks: List[int] = [1164727, 2809, 756894, 1323404, 886986, 1456346, 1725964, 1009001, 1589239, 701818] # top tsx mining companies
ciks: List[int] = [1652044, 789019, 1045810] # Google, Microsoft, NVidia,
# ciks: List[int] = [1009001]
print(f"ciks: {ciks}")
adsh_values: List[str] = []
dimh_values: List[str] = []
# sql_ciks: str = "(" + ", ".join(map(str, ciks)) + ")"


def save_or_append_dataframe(df: pd.DataFrame, name: str):
    """
    Saves a DataFrame to a file or appends it if the file exists.
    Removes duplicate rows before saving/appending.

    Args:
        df (pandas.DataFrame): The DataFrame to save or append.
        filepath (str): The path to the file.
    """
    filepath = f"../data/tables/{name}.csv"
    df = df.drop_duplicates()  # Remove duplicates before anything else.

    if os.path.exists(filepath):
        try:
            existing_df = pd.read_csv(filepath)
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            combined_df = (
                combined_df.drop_duplicates()
            )  # Remove duplicates after combining
            combined_df.to_csv(filepath, index=False)
        except pd.errors.EmptyDataError:  # Handle empty files.
            df.to_csv(filepath, index=False)
        except Exception as e:
            print(f"Error appending to file: {e}")

    else:
        try:
            df.to_csv(filepath, index=False)
        except Exception as e:
            print(f"Error saving to file: {e}")

def load_fixed_columns_tsv_from_file(tsv_file, num_columns=20):
    """
    Loads a TSV file with a fixed number of columns into a pandas DataFrame.

    Args:
        file_path (str): The path to the TSV file.
        num_columns (int): The expected number of columns in the TSV.

    Returns:
        pandas.DataFrame: The DataFrame, or None if an error occurs.
    """
    try:
        tsv_content = tsv_file.read().decode('utf-8')
        lines = tsv_content.splitlines() # split into lines

        data = []
        for line in lines:
            row = line.strip().split('\t')
            if len(row) > num_columns:
                row = row[:num_columns - 1] + ['\t'.join(row[num_columns - 1:])]
            # elif len(row) < num_columns:
            #     print(f"Warning: line with {len(row)} columns instead of {num_columns}")
            #     continue #skip row.

            data.append(row)
        # print(data[:5])
        columns = data[0]
        data = data[1:]
        df = pd.DataFrame(data, columns=columns)
        # print(df.head())
        # sys.exit()
        return df

    except FileNotFoundError:
        print(f"Error: File not found at {tsv_file}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred: {e}")
        return pd.DataFrame()

def convert_yyyymmdd(date_str):
    """Converts a YYYYMMDD string to a datetime.date object."""
    try:
        return pd.to_datetime(date_str, format='%Y%m%d', errors='coerce').date()
    except (ValueError, TypeError):
        return None  # Or pd.NaT, depending on your preference

def load_cik(tsv_file, quarter: str, table: str) -> pd.DataFrame:
    global ciks, adsh_values, dimh_values
    query: str = ""
    df: pd.DataFrame = pd.DataFrame()

    match table:
        case "sub":
            query = f"cik in {ciks}"
        case "tag":
            query = f"version in {adsh_values}"
        case "num" | "txt" | "ren" | "pre" | "cal":
            query = f"adsh in {adsh_values}"
        case "dim":
            query = f"dimhash in {dimh_values}"
        case _:
            raise ValueError(f"Invalid table: {table}")
    if (query.startswith("adsh") or query.startswith("version")) and len(adsh_values) == 0:
        return pd.DataFrame()
    if query.startswith("dimhash")  and len(dimh_values) == 0: 
        return pd.DataFrame()
    match table:
        case "tag":
            df = load_fixed_columns_tsv_from_file(tsv_file, num_columns=9)
        case "txt":
            df = load_fixed_columns_tsv_from_file(tsv_file, num_columns=20)
        case "num":
            df = pd.read_csv(tsv_file, sep='\t', converters={'date_str': convert_yyyymmdd})  # Read the .tsv file
        case _:
            df = pd.read_csv(tsv_file, sep='\t')  # Read the .tsv file
    df_sub = df.query(query)
    match table:
        case "sub":
            adsh_values = df_sub["adsh"].tolist()
        case "num":
            dimh_set = set(df_sub['dimh'])
            dimh_set.discard('0x00000000')
            dimh_values = list(dimh_set)
            # print(f"{dimh_values=}")
        case _:
            pass
    print(quarter, table, len(df), len(df_sub))
    save_or_append_dataframe(df_sub, table)
    return df_sub

tables = ["tag", "dim", "txt", "ren", "pre", "cal"]
table_files = [table + ".tsv" for table in tables]

def get_string_before_last_underscore(input_string):
    """
    Returns the portion of the input string up to (but not including) the last underscore.

    Args:
        input_string (str): The input string.

    Returns:
        str: The substring before the last underscore, or the original string if no underscore is found.
    """
    last_underscore_index = input_string.rfind("_")  # Find the last occurrence of "_"

    if last_underscore_index != -1:  # Check if an underscore was found
        return input_string[:last_underscore_index]
    else:
        return input_string  # Return the original string if no underscore is present



# %% Cell 2
# Transforms original SEC data into csv files for CIKs in ciks list
directory = "/mnt/usb-TOSHIBA_External_USB_3.0_20141121000522F-0:0-part1/sullija/sec_data"
for filename in os.listdir(directory):
    zn = get_string_before_last_underscore(filename)
    print(f"\r\n{zn=}")
    adsh_values = []
    dimh_values = []
    if filename.endswith(".zip"):
        zip_path = os.path.join(directory, filename)
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                    if file_info.filename == "sub.tsv":
                        fn = file_info.filename.split(".")[0]
                        with zip_ref.open(file_info) as tsv_file:
                            try:
                                load_cik(tsv_file, zn, fn)
                            except Exception as e:
                                print(f"An unexpected error occurred while processing {filename}: {file_info.filename}. Error: {e}")
                for file_info in zip_ref.infolist():
                    if file_info.filename == "num.tsv":
                        fn = file_info.filename.split(".")[0]
                        with zip_ref.open(file_info) as tsv_file:
                            try:
                                load_cik(tsv_file, zn, fn)
                            except Exception as e:
                                print(f"An unexpected error occurred while processing {filename}: {file_info.filename}. Error: {e}")                
                for file_info in zip_ref.infolist():
                    if file_info.filename in table_files:
                        fn = file_info.filename.split(".")[0]
                        with zip_ref.open(file_info) as tsv_file:
                            try:
                                load_cik(tsv_file, zn, fn)
                            except pd.errors.EmptyDataError:
                                print(f"Warning: Empty TSV file found in {filename}: {file_info.filename}")
                            except pd.errors.ParserError as e:
                                print(f"Error parsing TSV in {filename}: {file_info.filename}. Error: {e}")
                            except Exception as e:
                                print(f"An unexpected error occurred while processing {filename}: {file_info.filename}. Error: {e}")
        except zipfile.BadZipFile:
            print(f"Warning: Bad zip file: {filename}")
        except Exception as e:
            print(f"An unexpected error occurred while processing {filename}. Error: {e}")


# %% Cell 3
import duckdb
import os
import pandas as pd

def load_csvs_to_duckdb(data_dir="../data/tables/", database_name="../db.duckdb"):
    """
    Loads all CSV files from a folder into a DuckDB database.

    Args:
        data_dir (str): The path to the folder containing the CSV files.
        database_name (str): The name of the DuckDB database to create or connect to.
    """
    conn = duckdb.connect(database=database_name)
    sec_pk = {'sub': 'adsh', 'tag': 'tag, version', 'ren': 'adsh, report', 'pre': 'adsh, report, line', 'cal': 'adsh, grp, arc', 'dim': 'dimhash'}
    sec_date_column =  {'sub': ['changed', 'period', 'filed', 'floatdate'], 'num': ['ddate'], 'txt': ['ddate']} 


    # List all files in the specified directory
    all_files = os.listdir(data_dir)
    csv_files = [f for f in all_files if f.endswith('.csv')]
    # Iterate through the CSV files and load them into DuckDB tables
    for file_name in csv_files:
        file_path = os.path.join(data_dir, file_name)
        table_name = os.path.splitext(file_name)[0]  # Use the filename (without extension) as the table nam
        try:
            conn.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv('{file_path}', AUTO_DETECT=TRUE);""")
            if table_name in sec_date_column:
                strptimes: str = ""
                for col in sec_date_column[table_name]:
                    sql_alter = f"ALTER TABLE {table_name} ALTER COLUMN {col} TYPE DATE USING STRPTIME(CAST({col} AS BIGINT)::VARCHAR, '%Y%m%d')::DATE;"
                    print(sql_alter)
                    conn.execute(sql_alter)
            if table_name in sec_pk:
                # Add primary key constraint
                sql_pk = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({sec_pk[table_name]});"
                print(sql_pk)
                conn.execute(sql_pk)
            print(f"Loaded '{file_name}' into  view '{table_name}'")
        except Exception as e:
            print(f"Error loading '{file_name}': {e}")

    conn.close()

name = 'alphabet-ms-nvidia'
load_csvs_to_duckdb(data_dir=f"../data/tables_{name}", database_name=f"../{name}.duckdb")




# %% Cell 4
import sqlite3
import os
import pandas as pd

def load_csvs_to_sqlite(data_dir="../data/tables/", database_name="../db.sqlite"):
    """
    Loads all CSV files from a folder into a SQLite database using pandas.
    Creates tables with primary keys (if defined in sec_pk) on initial load
    and attempts to convert YYYYMMDD date columns.

    Args:
        data_dir (str): The path to the folder containing the CSV files.
        database_name (str): The name of the SQLite database to create or connect to.
    """
    conn = None
    try:
        # Connect to the SQLite database (creates it if it doesn't exist)
        conn = sqlite3.connect(database_name)
        cursor = conn.cursor()

        # Dictionary mapping table names to primary key column(s)
        # These will be used with pandas to_sql to create the PK on initial table creation
        sec_pk = {'sub': ['adsh'],
                  'tag': ['tag', 'version'],
                  'ren': ['adsh', 'report'],
                  'pre': ['adsh', 'report', 'line'],
                  'cal': ['adsh', 'grp', 'arc'],
                  'dim': ['dimhash']}

        # Dictionary mapping table names to date columns (expected in YYYYMMDD format)
        sec_date_column = {'sub': ['changed', 'period', 'filed', 'floatdate'],
                           'num': ['ddate'],
                           'txt': ['ddate']}

        # List all files in the specified directory
        all_files = os.listdir(data_dir)
        csv_files = [f for f in all_files if f.endswith('.csv')]

        # Iterate through the CSV files and load them into SQLite tables
        for file_name in csv_files:
            file_path = os.path.join(data_dir, file_name)
            table_name = os.path.splitext(file_name)[0]  # Use the filename (without extension) as the table name

            print(f"Processing '{file_name}' into table '{table_name}'...")

            # Check if the table already exists in the database
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            table_exists = cursor.fetchone() is not None

            if not table_exists:
                # If table does not exist, read CSV and load into the database
                print(f"Table '{table_name}' does not exist. Creating and loading data.")
                try:
                    # Read the CSV into a pandas DataFrame
                    df = pd.read_csv(file_path)

                    # --- Handle Primary Key Creation during to_sql ---
                    pk_cols = sec_pk.get(table_name)
                    if pk_cols:
                        # Check if PK columns exist in DataFrame
                        if all(col in df.columns for col in pk_cols):
                             print(f"Attempting to create table '{table_name}' with primary key on: {', '.join(pk_cols)}")
                             try:
                                 # Set columns as DataFrame index for to_sql to create PK
                                 # Use inplace=False to avoid modifying the original DataFrame in place
                                 df_with_index = df.set_index(pk_cols)
                                 # to_sql will create the PK from the index
                                 df_with_index.to_sql(table_name, conn, if_exists='replace', index=True)
                                 print(f"Table '{table_name}' created successfully with primary key.")
                             except Exception as e:
                                 print(f"Error setting index or creating table with PK for '{table_name}': {e}")
                                 # If setting index or creating with PK fails, fall back to creating without PK
                                 print("Falling back to creating table without primary key constraint initially.")
                                 df.to_sql(table_name, conn, if_exists='replace', index=False)
                                 print(f"Table '{table_name}' created without primary key.")
                        else:
                             missing_cols = [col for col in pk_cols if col not in df.columns]
                             print(f"Warning: Primary key columns {missing_cols} not found in '{file_name}'. Creating table without primary key.")
                             # Create table without PK if columns are missing in CSV
                             df.to_sql(table_name, conn, if_exists='replace', index=False)
                             print(f"Table '{table_name}' created without primary key.")
                    else:
                        # No primary key defined for this table, create without PK
                        print(f"No primary key defined for table '{table_name}'. Creating table without primary key definition.")
                        df.to_sql(table_name, conn, if_exists='replace', index=False)
                        print(f"Table '{table_name}' created.")

                    # --- End Primary Key Creation ---

                    print(f"Data loaded successfully into table '{table_name}'.")

                    # --- Handle Date Conversion after initial load ---
                    # Date conversion needs to happen after data is in the table
                    if table_name in sec_date_column:
                        print(f"Attempting to convert date columns for table '{table_name}'...")
                        for col in sec_date_column[table_name]:
                             # Check if the column exists in the table
                             cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
                             cols_info = cursor.fetchall()
                             col_names = [info[1] for info in cols_info]

                             if col in col_names:
                                  # SQL to convert YYYYMMDD (integer or text) to YYYY-MM-DD text format
                                  # SQLite's date functions work well with 'YYYY-MM-DD' text.
                                  # We use CAST to TEXT to ensure substr works, regardless of storage class.
                                  # The WHERE clause attempts to only process values that look like an 8-digit date.
                                  sql_update_date = f"""
                                  UPDATE \"{table_name}\"
                                  SET \"{col}\" = printf('%s-%s-%s',
                                                   substr(CAST(\"{col}\" AS TEXT), 1, 4),
                                                   substr(CAST(\"{col}\" AS TEXT), 5, 2),
                                                   substr(CAST(\"{col}\" AS TEXT), 7, 2))
                                  WHERE TYPEOF(\"{col}\") IN ('integer', 'text') AND LENGTH(CAST(\"{col}\" AS TEXT)) = 8;
                                  """
                                  # print(f"Executing date conversion SQL: {sql_update_date}") # Uncomment for debugging
                                  try:
                                       cursor.execute(sql_update_date)
                                       conn.commit() # Commit the update changes
                                       print(f"Converted date column '{col}' in table '{table_name}' to YYYY-MM-DD format.")
                                  except Exception as e:
                                       print(f"Error converting date column '{col}' in table '{table_name}': {e}")
                             else:
                                  print(f"Warning: Date column '{col}' not found in table '{table_name}'. Skipping date conversion.")
                    # --- End Date Conversion ---

                except Exception as e:
                    print(f"Error processing '{file_name}': {e}")
                    continue # Skip to the next file

            else:
                # If table exists, skip data loading based on the original IF NOT EXISTS logic.
                # However, we should still attempt date conversion in case it failed before.
                print(f"Table '{table_name}' already exists. Skipping data loading.")

                # --- Attempt Date Conversion if table exists ---
                # This block is the same as the one in the 'if not table_exists' block
                if table_name in sec_date_column:
                    print(f"Attempting to convert date columns for table '{table_name}'...")
                    for col in sec_date_column[table_name]:
                         cursor.execute(f"PRAGMA table_info(\"{table_name}\");")
                         cols_info = cursor.fetchall()
                         col_names = [info[1] for info in cols_info]

                         if col in col_names:
                              sql_update_date = f"""
                              UPDATE \"{table_name}\"
                              SET \"{col}\" = printf('%s-%s-%s',
                                               substr(CAST(\"{col}\" AS TEXT), 1, 4),
                                               substr(CAST(\"{col}\" AS TEXT), 5, 2),
                                               substr(CAST(\"{col}\" AS TEXT), 7, 2))
                              WHERE TYPEOF(\"{col}\") IN ('integer', 'text') AND LENGTH(CAST(\"{col}\" AS TEXT)) = 8;
                              """
                              try:
                                   cursor.execute(sql_update_date)
                                   conn.commit()
                                   print(f"Converted date column '{col}' in table '{table_name}'.")
                              except Exception as e:
                                   print(f"Error converting date column '{col}' in table '{table_name}': {e}")
                         else:
                              print(f"Warning: Date column '{col}' not found in table '{table_name}'. Skipping date conversion.")
                # --- End Date Conversion if table exists ---


            print(f"Finished processing table '{table_name}'.")

    except sqlite3.Error as se:
        print(f"A SQLite database error occurred: {se}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Ensure the connection is closed even if errors occur
        if conn:
            conn.close()
            print("Database connection closed.")

# Example usage:
# Replace 'alphabet-ms-nvidia' with the desired name prefix for your data directory and database file.
name = 'alphabet-ms-nvidia'
# Define the path to the directory containing your CSV files
data_dir_path = f"../data/tables_{name}"
# Define the name for your SQLite database file
database_file_name = f"../{name}.sqlite" # Changed to .sqlite extension

# Check if the data directory exists before attempting to process
if not os.path.isdir(data_dir_path):
    print(f"Error: Data directory not found at {data_dir_path}")
else:
    # Call the function to load the data
    load_csvs_to_sqlite(data_dir=data_dir_path, database_name=database_file_name)


# %% Cell 5
import os
import csv
import psycopg # Import psycopg v3

def infer_postgres_type(value):
    """
    Simple type inference for PostgreSQL based on a single value.
    This is a basic implementation and might need refinement
    depending on the complexity and consistency of your CSV data.
    It prefers numeric types and defaults to TEXT.
    """
    if value is None or value == '':
        # PostgreSQL allows NULLs in any type, we can't infer type from nulls.
        # Returning TEXT as a safe default if only seen nulls in sample,
        # but the actual type will be determined by the first non-null value or header analysis.
        # For COPY FROM, initial table creation needs a best guess.
        # Let's rely on seeing non-empty values in sample for better inference.
        return None # Indicate ambiguity, should not happen if sample_rows has data

    try:
        # Try integer first (covers values like '123', '20231231')
        # Check if the value is purely numeric (contains only digits and maybe a sign)
        if str(value).lstrip('-+').isdigit():
             # Consider date format YYYYMMDD as a special case for potential date conversion later
            if len(str(value).strip()) == 8: # Simple check for 8-digit numbers
                 return 'BIGINT' # Keep as BIGINT initially, convert to DATE in Phase 3
            return 'BIGINT' # Use BIGINT for larger integer ranges
    except ValueError:
        pass # Not an integer

    try:
        # Try float/numeric (covers values like '123.45')
        float(value)
        return 'NUMERIC' # Use NUMERIC for precision
    except ValueError:
        pass # Not a number

    # If it's not clearly numeric, treat as text
    return 'TEXT'


def load_csvs_to_postgres_psycopg(data_dir="../data/tables/",
                                   db_name="your_postgres_db",
                                   db_user="your_postgres_user",
                                   db_password="your_postgres_password",
                                   db_host="localhost",
                                   db_port="5432"):
    """
    Loads all CSV files from a folder into a PostgreSQL database using psycopg (v3).

    Args:
        data_dir (str): The path to the folder containing the CSV files.
        db_name (str): PostgreSQL database name.
        db_user (str): PostgreSQL user name.
        db_password (str): PostgreSQL password.
        db_host (str): PostgreSQL host.
        db_port (str): PostgreSQL port.
    """
    conn_params = {
        "dbname": db_name,
        "user": db_user,
        "password": db_password,
        "host": db_host,
        "port": db_port,
    }

    # Mappings for primary keys and date columns - Keep these
    sec_pk = {'sub': 'adsh', 'tag': 'tag, version', 'ren': 'adsh, report', 'pre': 'adsh, report, line', 'cal': 'adsh, grp, arc', 'dim': 'dimhash'}
    sec_date_column = {'sub': ['changed', 'period', 'filed', 'floatdate'], 'num': ['ddate'], 'txt': ['ddate']}

    # List all files in the specified directory
    try:
        all_files = os.listdir(data_dir)
        csv_files = [f for f in all_files if f.endswith('.csv')]
    except FileNotFoundError:
        print(f"Error: Data directory not found at '{data_dir}'.")
        return

    # Establish connection outside the loop for efficiency, but manage transactions per file
    conn = None
    try:
        # Connect to PostgreSQL database
        conn = psycopg.connect(**conn_params)
        # Set autocommit to False to manage transactions manually per file
        conn.autocommit = False
        print("Successfully connected to PostgreSQL.")

        for file_name in csv_files:
            file_path = os.path.join(data_dir, file_name)
            # Use the filename (without extension) as the table name.
            # Convert to lowercase as PostgreSQL table names are typically lowercase by default.
            table_name = os.path.splitext(file_name)[0].lower()

            print(f"Processing '{file_name}' into table '{table_name}'...")

            try:
                # Use a 'with' block for the cursor, it will be closed automatically
                with conn.cursor() as cur:

                    # --- Phase 1: Schema Inference and Table Creation ---
                    # Read header and infer types by sampling data
                    columns = []
                    column_types = {}
                    sample_size = 100 # Number of rows to sample for type inference

                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            reader = csv.reader(f)
                            try:
                                header = next(reader) # Read header row
                                columns = [col.strip() for col in header] # Get column names
                            except StopIteration:
                                print(f"Warning: File '{file_name}' is empty or has no header. Skipping.")
                                continue # Skip to the next file

                            # Read sample rows to infer types more accurately
                            sample_rows = []
                            try:
                                for i in range(sample_size):
                                    row = next(reader)
                                    # Ensure row has enough columns before appending
                                    if len(row) == len(columns):
                                         sample_rows.append(row)
                                    else:
                                        # Handle potential malformed rows in sample
                                        print(f"Warning: Sample row {i+2} in '{file_name}' has inconsistent column count ({len(row)} instead of {len(columns)}). Skipping this row for inference.")

                            except StopIteration:
                                # File has fewer rows than sample_size
                                pass

                            # Infer types based on sampled data.
                            # Initialize with a default (like TEXT), then refine.
                            # If no sample rows, all will remain TEXT.
                            for col in columns:
                                column_types[col] = 'TEXT' # Default type

                            if sample_rows:
                                for i, col in enumerate(columns):
                                    # Collect non-empty values for this column across sample rows
                                    col_values = [row[i].strip() for row in sample_rows if i < len(row) and row[i].strip() != '']
                                    if col_values: # Only infer if there are non-empty values
                                        # Check if ALL non-empty values in the sample match a type
                                        inferred_type = None
                                        for val in col_values:
                                            current_val_type = infer_postgres_type(val)
                                            if inferred_type is None:
                                                inferred_type = current_val_type
                                            elif inferred_type != current_val_type:
                                                # Found conflicting types in sample, default to TEXT
                                                inferred_type = 'TEXT'
                                                break # Stop checking values for this column
                                        if inferred_type is not None: # Update type if inference was successful
                                             column_types[col] = inferred_type
                                        else: # Should not happen with current infer_postgres_type
                                             column_types[col] = 'TEXT' # Fallback just in case

                            # IMPORTANT: Reset file pointer to the beginning for the COPY operation
                            f.seek(0)


                    except FileNotFoundError:
                        print(f"Error: File not found at '{file_path}'. Skipping.")
                        continue # Skip to next file
                    except csv.Error as e:
                         print(f"Error reading CSV header or sample rows from '{file_name}': {e}. Skipping.")
                         continue # Skip to next file
                    except Exception as e:
                         print(f"An unexpected error occurred during schema inference for '{file_name}': {e}. Skipping.")
                         continue


                    # Construct CREATE TABLE statement
                    # Decide on column definitions based on inferred types
                    columns_definition = []
                    for col in columns:
                         # Quote column name for safety
                        col_def = f'"{col}" {column_types[col]}'
                        columns_definition.append(col_def)

                    if not columns_definition:
                        print(f"Warning: No columns inferred for '{file_name}'. Skipping table creation.")
                        continue # Skip to next file

                    # Drop table if it exists to ensure clean load (similar to DuckDB behavior)
                    drop_table_sql = f'DROP TABLE IF EXISTS "{table_name}";'
                    print(f"Executing: {drop_table_sql}")
                    cur.execute(drop_table_sql)

                    create_table_sql = f"""
                        CREATE TABLE "{table_name}" (
                            {", ".join(columns_definition)}
                        );
                    """
                    print(f"Executing: {create_table_sql}")
                    cur.execute(create_table_sql)
                    print(f"Created table '{table_name}' with inferred schema.")

                    # --- Phase 2: Data Loading using COPY FROM ---
                    # Use COPY FROM STDIN for efficient loading from file object
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            # The copy_from method handles the COPY command internally.
                            # We provide the file object, table name, columns, format, and header option.
                            print(f"Executing COPY FROM for '{file_name}'...")
                            cur.copy_from(
                                f,                  # File-like object
                                table_name,         # Target table name
                                columns=columns,    # List of column names (matches CSV header)
                                format='csv',       # File format
                                header=True         # Skip the header row in the file
                            )

                        print(f"Loaded data from '{file_name}' into table '{table_name}' using COPY FROM.")

                    except Exception as e:
                        print(f"Error during COPY FROM for '{file_name}': {e}")
                        raise # Re-raise to trigger rollback


                    # --- Phase 3: Data Type Conversion (ALTER TABLE for Dates) ---
                    if table_name in sec_date_column:
                        print(f"Converting date columns for table '{table_name}'...")
                        for col in sec_date_column[table_name]:
                            # Check if column exists based on our header columns
                            if col in columns:
                                try:
                                    # PostgreSQL TO_DATE function expects text. Cast if necessary.
                                    # Assume date format is YYYYMMDD and stored as TEXT or BIGINT
                                    sql_alter = f"""
                                        ALTER TABLE "{table_name}"
                                        ALTER COLUMN "{col}" TYPE DATE
                                        USING TO_DATE(CAST("{col}" AS TEXT), 'YYYYMMDD');
                                    """
                                    print(f"Executing: {sql_alter}")
                                    cur.execute(sql_alter)
                                    print(f"Converted column '{col}' to DATE.")
                                except Exception as e:
                                    print(f"Warning: Could not convert column '{col}' to DATE for table '{table_name}': {e}")
                            else:
                                 print(f"Warning: Date column '{col}' not found in CSV header for table '{table_name}'. Skipping conversion.")


                    # --- Phase 4: Add Primary Key ---
                    if table_name in sec_pk:
                        pk_columns_str = sec_pk[table_name]
                        pk_columns_list = [col.strip() for col in pk_columns_str.split(',')]

                        # Verify primary key columns exist based on header columns
                        if all(col in columns for col in pk_columns_list):
                            print(f"Adding primary key constraint for table '{table_name}' on columns: {pk_columns_str}")
                            try:
                                # Add primary key constraint - requires columns to be NOT NULL and unique.
                                # You might need to add ALTER COLUMN ... SET NOT NULL first if they are nullable
                                # and ensure data has no duplicates for the PK columns.
                                # For simplicity here, we add the PK constraint directly.
                                # Quoting column names.
                                quoted_pk_columns = ', '.join([f'"{c}"' for c in pk_columns_list])
                                sql_pk = f"""
                                    ALTER TABLE "{table_name}"
                                    ADD PRIMARY KEY ({quoted_pk_columns});
                                """
                                print(f"Executing: {sql_pk}")
                                cur.execute(sql_pk)
                                print(f"Added primary key to '{table_name}'.")
                            except Exception as e:
                                # Catch errors if PK cannot be added (e.g., duplicate keys, nulls)
                                print(f"Warning: Could not add primary key to '{table_name}': {e}")
                        else:
                             print(f"Warning: Primary key columns '{pk_columns_str}' not all found in CSV header for table '{table_name}'. Skipping primary key constraint.")

                # If everything in the try block within the loop succeeded, commit the transaction
                conn.commit()
                print(f"Successfully processed and committed '{file_name}'.")

            except Exception as e:
                # Catch any exception that occurred within the file processing try block
                print(f"An error occurred while processing '{file_name}'. Rolling back transaction: {e}")
                conn.rollback() # Rollback the transaction for this file


    except Exception as e:
         print(f"Error establishing initial database connection or listing files: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


# Example usage:
# Replace with your actual PostgreSQL database credentials and desired database name, host, and port.
# You need to have the database created in PostgreSQL beforehand.

name = 'alphabet-ms-nvidia'
load_csvs_to_postgres_psycopg(data_dir=f"../data/tables_{name}",
                               db_name="fin_report",
                               db_user="puser",
                               db_password=os.environ.get('AKASAKA_DB_PW', 'default_password'),
                               db_host="localhost", # e.g., 'localhost' or an IP address
                               db_port="5432") # e.g., '5432' is the default PostgreSQL port


# %% Cell 6
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
