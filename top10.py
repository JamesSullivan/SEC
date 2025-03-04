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
