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

def load_csvs_to_duckdb(folder_path="../data/tables", database_name="../db.duckdb"):
    """
    Loads all CSV files from a folder into a DuckDB database.

    Args:
        folder_path (str): The path to the folder containing the CSV files.
        database_name (str): The name of the DuckDB database to create or connect to.
    """

    try:
        con = duckdb.connect(database=database_name)

        for filename in os.listdir(folder_path):
            if filename.endswith(".csv"):
                table_name = os.path.splitext(filename)[0]  # Remove .csv extension
                file_path = os.path.join(folder_path, filename)

                # Option 1: Using COPY FROM (Faster for large files)
                con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_csv('{file_path}', AUTO_DETECT=TRUE);")

                # Alternative option 2: Using pandas (For more complex transformations or smaller files)
                # df = pd.read_csv(file_path)
                # con.register(table_name, df)
                # con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {table_name};")
                # con.unregister(table_name) #unregister the pandas dataframe after copying to duckdb.

        con.close()
        print(f"CSV files from '{folder_path}' loaded into DuckDB database '{database_name}'.")

    except Exception as e:
        print(f"An error occurred: {e}")
        if 'con' in locals():
            con.close()

name = 'google_ms_nvidia'
load_csvs_to_duckdb(folder_path=f"../data/tables_{name}", database_name=f"../db_{name}.duckdb")
# %%
