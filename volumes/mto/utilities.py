# -*- coding: utf-8 -*-
"""
Created on Thu Jul  6 16:13:13 2017

@author: qwang2
"""
import sys
import os
for x in os.walk('.'):
    sys.path.append(x[0]) 
    
from pg import DB
from pg import ProgrammingError
import configparser
import pandas as pd

def get_sql_results(self, filename, columns, parameters=None):
        
        f = None
        try:
            f = open(filename)
        except:
            for root_f, folders, files in os.walk('.'):
                if filename in files:
                    f = open(root_f + '/' + filename)
                    
        if f is None:
            if filename[:6] == 'SELECT': # Also accepts sql queries directly in string form
                sql = filename
            else:
                raise Exception ('File not found!')
        else:    
            sql = f.read()

        try:
            if parameters is not None:
                return pd.DataFrame(self.db.query(sql, parameters).getresult(), columns = columns)
            else:
                return pd.DataFrame(self.db.query(sql).getresult(), columns = columns)
        except ProgrammingError as pe:
            print(pe)
            
def db_connect():
    CONFIG = configparser.ConfigParser()
    CONFIG.read('db.cfg')
    dbset = CONFIG['DBSETTINGS']
    db = DB(dbname=dbset['database'],host=dbset['host'],user=dbset['user'],passwd=dbset['password'])
    return db