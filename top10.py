# %% Cell 1
from typing import List

# %% Cell 2
num: List[int] = [
    1164727,
    2809,
    756894,
    1323404,
    886986,
    1456346,
    1725964,
    1009001,
    1589239,
    701818,
]
str_num: List[str] = list(map(lambda n: f"{n:0>10}", num))
# print(str_num)
# for s in str_num:
# print("'" + s)

for n in num:
    print(f"{n}, ", end="")
# %% Cell 3
from tabulate import tabulate
import pandas as pd

# creating a DataFrame
dict = {
    "Name": ["Martha", "Tim", "Rob", "Georgia"],
    "Maths": [87, 91, 97, 95],
    "Science": [83, 99, 84, 76],
}
df = pd.DataFrame(dict)
print(df)
# displaying the DataFrame
print(tabulate(df, headers="keys", tablefmt="psql"))
