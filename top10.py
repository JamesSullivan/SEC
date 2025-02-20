# %% Cell 1
from typing import List
# %% Cell 2
num: List[int] = [1164727, 2809, 764022, 1340496, 883015, 1430067, 1695295, 1009003, 1158041, 701818]
str_num: List[str] = list(map(lambda n: f"{n:0>10}", num))
# print(str_num)
for s in str_num:
    print("'" + s)
