# -*- coding: utf-8 -*-
"""
Created on Wed Sep  5 14:14:46 2018

@author: BJoseph
"""

import pandasdb
from sqlalchemy import Column, Integer, String, Float, DateTime
import datetime
import xml.etree.ElementTree as ET #module for parsing XML files
import requests #module for wget of xml file
import os 
import pandas as pd
import schedule
import time

class sched_act():
    
    def testing(self):
        print('testing')

    def call_wget(self):
        date_dt = datetime.datetime.now() + datetime.timedelta(hours=-1)
        
        ## Check if hour is 00 then convert to IESO format
        if date_dt.hour == 0: 
            date_dt = date_dt + datetime.timedelta(hours=-1)
            date_str = datetime.datetime.strftime(date_dt, "%Y%m%d")
            date_str = date_str + '24'
        else:
            date_str = datetime.datetime.strftime(date_dt, "%Y%m%d%H")
        
        url = f'http://reports.ieso.ca/public/RealtimeConstTotals/PUB_RealtimeConstTotals_{date_str}.xml'
        xml_direct = 'data'
        xml_file = 'iesoactual.xml'
        
        response = requests.get(url)
        xml_loc = os.path.join(xml_direct, xml_file)
        with open(xml_loc, 'wb') as f:
            f.write(response.content) 
    
        #Parse XML File
        
        tree = ET.parse(xml_loc)
        root = tree.getroot()
        
        demand = []
        for i in range(12):
            demand.append(float(root[1][2][i][8][1].text))  #[0][8][1].text#root for today's prediction
        total_demand = [sum(demand)/len(demand)]
        
        timeless_dt = [datetime.datetime(year=date_dt.year, month=date_dt.month, day=date_dt.day, hour=date_dt.hour)]
        df = pd.DataFrame({'Date/Time': timeless_dt, 'IESO Actual Demand': total_demand})
        
        #To Database
        database = 'bjos'
        table = 'IESOACTUAL'
        dtypes = [DateTime(), Float()]
        db = pandasdb.pandasdb(database, table)
        db.pd_to_db(dtypes, df, if_exists='append')
   
    def sched(self):
        schedule.every().hour.do(self.call_wget)
        while 1:
            schedule.run_pending()
            time.sleep(1)
            
if __name__ == "__main__":
    obj = sched_act()
    obj.sched()
    
