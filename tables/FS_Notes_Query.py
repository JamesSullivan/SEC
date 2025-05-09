
# %% Cell 1
import os
import pandas as pd
import psycopg
from sqlalchemy import create_engine, text

class fin_report():
                        
    def __init__(self):
        password = os.environ.get('AKASAKA_DB_PW', 'default_password')
        # Set up the connection to PostgreSQL
        self.engine = create_engine(f'postgresql://puser:{password}@localhost:5432/finreport') 

        with self.engine.connect() as connection:
            result = connection.execute(text("select username from users"))
            for row in result:
                print("username:", row.username)



