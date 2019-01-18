# -*- coding: utf-8 -*-
"""
Data naar de VAO brengen

"""

import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import re
# of import sqlite3
#import MySQLdb as mdb 

#import datetime #, time


file = 'G:/OIS/Beveiligde Projecten/18152C Zicht en Grip op Sociaal - Gepseudonimiseerde data/data/BRP/bbbbvk17k1.hash'

class DataImporter():
        
    def __init__(self, serverType = 'postgresql', user='hhofstetter', pw = 'zicht', host = 'db.zichtengrip.vao.amsterdam', port = 5432, database = 'dbzichtengrip'):
        #self.file = file
        self.serverType = serverType
        self.user = user
        self.pw = pw
        self.host = host
        self.port = port
        self.database = database
        
        self.engine = None
        self.conn = None
        self.cur = None
        self.tableName = None
        self.schema = None
        self.file = None
        self.data = pd.DataFrame()
        self.sep = ';'

    def selectDataFile(self, file = 'G:/OIS/Beveiligde Projecten/18152C Zicht en Grip op Sociaal - Gepseudonimiseerde data/data/BRP/bbbbvk17k1.hash'):
        self.file = file
        if re.search(r'.csv', file) or re.search(r'.hash', file):
            self.data = pd.read_csv(file, sep = ';')
            if len(self.data.columns) < 2:
                self.data = pd.read_csv(file, sep = ',')
                self.sep = ','  
        elif re.search(r'tamtabel', file):
            self.data = pd.read_excel(file, sheetname = 'Voorzieningen', skiprows = 6)     
        else: 
            self.data = pd.read_excel(file)
        self.createCSV()
        return

    def createServerConnectionPandas(self):
        code = "%s://%s:%s@%s:%d/%s" % (self.serverType, self.user, self.pw, self.host, self.port, self.database)
        print(code)
        self.engine = create_engine(code) # dus: dialect+driver://username:password@host:port/database
        return 
    
    def createServerConnectionPython(self):
        code = "host=%s dbname=%s user=%s password=%s port=%s" % (self.host, self.database, self.user, self.pw, self.port)
        conn = psycopg2.connect(code)
        cur = conn.cursor()
        return conn, cur

    def createCSV(self):
        self.data.to_csv('temp.csv', sep = ';', index = False)
        self.file = 'temp.csv'
        return
    
    def createEmptyTable(self, tableName, schema = None):
        self.tableName = tableName
        self.schema = schema
        if not self.conn: self.conn, self.cur = self.createServerConnectionPython()
        print(self.data.dtypes)
        dtypeVertaling = {np.dtype('int64'):'bigint',np.dtype('O'):'text',np.dtype('float64'):'double precision',np.dtype('bool'):'boolean'}
        if schema:
            #try: 
            #    self.cur.execute("DROP TABLE " + schema + '.' + tableName)    
            #except:
            #    print('Tabel bestond nog niet in db')
            query = "CREATE TABLE " + schema + '.' + tableName + '('
        else:
            #self.cur.execute("DROP TABLE " + tableName)              
            query = "CREATE TABLE " + tableName + '('            
        for naam, dtype in zip(self.data.columns, self.data.dtypes):
            naam = naam.lower().replace(' ','_').replace('/','_').replace('-','_').replace('<','').replace('>','').replace('(','_').replace(')','')
            query += naam + ' ' + dtypeVertaling[dtype] + ','
        query = query.strip(',')
        query += ')'
        print(query)

        self.cur.execute(query)
        
        ## eerste 100 records uit dataframe wegschrijven        
        #if not self.engine: self.engine = self.createServerConnectionPandas()
        #if not self.data.empty:
        #    self.data.head(100).to_sql(tableName, con = self.engine, schema = schema, if_exists = 'replace', index = False)
        #else: 
        #    print('No Data to import')
        # leegmaken deze tabel
        #if not self.conn: self.conn, self.cur = self.createServerConnectionPython()
        #if schema:
        #    query = "TRUNCATE " + schema + '.'  + tableName  
        #else:
        #    query = "TRUNCATE " + tableName 
        #self.cur.execute(query)
        return
                
    def fillTable(self):
        with open(self.file, 'r', encoding = "utf8") as f:
            next(f)  # Skipt de rij met  kolomnamen
            if self.schema:
                self.cur.copy_from(f, self.schema + '.' + self.tableName, sep=self.sep, null = "")                  
            else:
                self.cur.copy_from(f, self.tableName, sep= self.sep)    
        self.conn.commit()        
        
    def importTable_fromCSV(self, file, tableName, schema = None):
        self.selectDataFile(file)
        self.createServerConnectionPandas()
        self.createServerConnectionPython()
        self.createEmptyTable(tableName, schema = schema)
        self.fillTable()

    def importTable_fromData(self, data, tableName, schema = None):
        self.data = data
        self.createCSV()
        self.createServerConnectionPandas()
        self.createServerConnectionPython()
        self.createEmptyTable(tableName, schema = schema)
        self.fillTable()
        

#def main():   http://localhost:8888/edit/distance_matrix_datapipeline/data_importer.py#
#    Importer = DataImporter(file = 'G:/OIS/Beveiligde Projecten/18152C Zicht en Grip op Sociaal - Gepseudonimiseerde data/data/BRP/bbbbvk17k1.hash')  
#    Importer.importTable_fromCSV('test_tabel')
    














"""
def run_sql_file(filename, connection):
    '''
    The function takes a filename and a connection as input
    and will run the SQL query on the given connection  
    '''
    file = open(filename, 'r')
    sql = " ".join(file.readlines())
    print("Start executing: " + filename + " at " + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M")) + "\n" + sql) 
    cursor = connection.cursor()
    cursor.execute(sql)    
    connection.commit() # weet niet zeker of moet
    
 
def run_sql_syntax(table, connection):    
    sql = "CREATE TABLE EMPLOYEE (
         FIRST_NAME  CHAR(20) NOT NULL,
         LAST_NAME  CHAR(20),
         AGE INT,  
         SEX CHAR(1),
         INCOME FLOAT )"   
    cursor = connection.cursor()  
    cursor.execute(sql)  
    
 
    
    
def main():    
    connection = mdb.connect('127.0.0.1', 'root', 'password', 'database_name')
    run_sql_file("my_query_file.sql", connection)    
    connection.close()
"""