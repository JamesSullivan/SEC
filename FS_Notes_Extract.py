import duckdb
import os
import pandas as pd
from IPython.display import display
from typing import List, Set, Dict, Tuple, Optional

ciks = [1009001]
sql_ciks: str = "(" + ", ".join(map(str, ciks)) + ")"
print(f"sql_ciks: {sql_ciks}")
# Connect to DuckDB (in-memory database by default)
con = duckdb.connect()
yearq = "2023q1"


def load_tsv(quarter, table):
    # Load the TSV file into a table ../2023q1_notes/sub.tsv
    df = pd.read_csv(f"../{quarter}_notes/{table}.tsv", delimiter="\t")
    return df


def save_or_append_dataframe(df: pd.DataFrame, name: str):
    """
    Saves a DataFrame to a file or appends it if the file exists.
    Removes duplicate rows before saving/appending.

    Args:
        df (pandas.DataFrame): The DataFrame to save or append.
        filepath (str): The path to the file.
    """
    filepath = f"./tables/{name}.csv"
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


df = load_tsv(yearq, "sub")
df_sub = df.query(f"cik in {ciks}")
print(len(df), len(df_sub))
adsh_values = df_sub["adsh"].tolist()
print(f"{adsh_values=}")
# print(f"{type(adsh_values)} {adsh_values}")
# duckdb.sql(f"SELECT * FROM df_sub WHERE cik IN {sql_ciks};")
save_or_append_dataframe(df_sub, "sub")
display(df_sub)
# filtered_df.iloc[:, :10]
