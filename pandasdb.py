# -*- coding: utf-8 -*-
"""
Created on Fri Aug 31 08:41:03 2018

@author: BJoseph
"""

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, Float, DateTime
import pandas as pd

class pandasdb():
    def __init__(self, database, table):
        self.database = database
        self.table = table
        
        self.engine = create_engine('postgresql://bjos:3iRM7Ihr@@localhost:5432/bjos')
    
#    def create_table(self):
#        connection = psycopg2.connect(host="localhost", database="bjos", user="bjos", password="3iRM7Ihr@")
#        cur = conn.cursor()
#        cur.execute(f'''CREATE TABLE {self.table}
#            (ID INT PRIMARY KEY     NOT NULL,
#            Date/Time TEXT          NOT NULL,
#            Temp
#            )
#        ''')
        
    def pd_to_db(self, dtypes, df, if_exists):
        if len(dtypes) != len(df.columns):
            print('len of dtypes must equal number of columns')
        else:
            dtypes_dict = dict(zip(list(df.columns), dtypes))
            print(dtypes_dict)
        
            df.to_sql(name = self.table, con = self.engine, if_exists = if_exists, index = False, dtype = dtypes_dict)

        
    def pd_from_db(self):
        df = pd.read_sql(f'SELECT * FROM {self.table}', con = self.engine)
        
        return df