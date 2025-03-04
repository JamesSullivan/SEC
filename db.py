# repl: run or ctrl-shift-enter
# %% Cell 1
import duckdb
import pandas as pd
from IPython.display import display

df: pd.DataFrame = pd.read_csv("../2024q4/sub.txt", delimiter="\t")
display(df.head(10))


# %% Cell 2
display(
    duckdb.sql(
        "SELECT * FROM df WHERE cik IN (1164727, 2809, 756894, 1323404, 886986, 1456346, 1725964, 1009001, 1589239, 701818);"
    )
)
