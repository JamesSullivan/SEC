# %% Cell 1
import os
import pandas as pd

from IPython.display import display, HTML
from typing import List, Set, Dict, Tuple, Optional

ciks: List[int] = [1164727, 2809, 756894, 1323404,
                   886986, 1456346, 1725964, 1009001, 1589239, 701818]
print(f"ciks: {ciks}")
adsh_values: List[str] = []
dimh_values: List[str] = []
# sql_ciks: str = "(" + ", ".join(map(str, ciks)) + ")"
yearq = "2023q4"


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


def load_tsv(quarter: str, table: str) -> pd.DataFrame:
    # Load the TSV file into a table ../2023q1_notes/sub.tsv
    return pd.read_csv(f"../{quarter}_notes/{table}.tsv", delimiter="\t")


def load_cik(quarter: str, table: str) -> pd.DataFrame:
    df = load_tsv(quarter, table)
    match table:
        case "sub":
            query = f"cik in {ciks}"
        case "tag":
            query = f"version in {adsh_values}"
        case "num" | "txt" | "ren" | "pre" | "cal":
            query = f"adsh in {adsh_values}"
        case "dim":
            query = f"dimhash in {dimh_values}"
        case "txt":
            query = f"adsh in {adsh_values}"
        case _:
            raise ValueError(f"Invalid table: {table}")
    df_sub = df.query(query)
    print(table, len(df), len(df_sub))
    save_or_append_dataframe(df_sub, table)
    return df_sub


# %% Cell 2
df = load_cik(yearq, "sub")
adsh_values = df["adsh"].tolist()
# print(f"{adsh_values=}")
df_num = load_cik(yearq, "num")
dimh_set = set(df_num['dimh'])
dimh_set.discard('0x00000000')
dimh_values = list(dimh_set)
# print(f"{dimh_set=}")
tables = ["tag", "dim", "txt", "ren", "pre", "cal"]
for table in tables:
    df = load_cik(yearq, table)  # print(f"{adsh_values=}")
# print(df.head())
