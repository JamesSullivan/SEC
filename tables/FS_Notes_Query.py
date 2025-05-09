
# %% Cell 1
import os
import pandas as pd
import psycopg
from sqlalchemy import create_engine, text, CursorResult
from typing import List

class fin_report():
                        
    def __init__(self):
        password: str = os.environ.get('AKASAKA_DB_PW', 'default_password')
        # Set up the connection to PostgreSQL
        self.engine = create_engine(f'postgresql://puser:{password}@localhost:5432/fin_report') 
        self.companies: List[str] = []
        with self.engine.connect() as connection:
            result: CursorResult = connection.execute(text("select distinct(name) from sub;"))
            for row in result:
                self.companies.append(row.name)



fr = fin_report()
print(fr.companies)

# %%
