#!/usr/bin/env python
# coding: utf-8

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, inspect
import mysql.connector
import psycopg2
import pandas as pd
import time


start_time = time.time()


postgres_engine = create_engine('postgresql://praveen:Admin123@165.22.220.96:5432/staging')


reports = pd.read_sql('SELECT * FROM stg_salesreports', con=postgres_engine).iloc[0,0]

last_value = pd.read_sql('SELECT max(sales_id) FROM stg_salesreports', con=postgres_engine).iloc[0,0]
last_value


### Establishing MySQL and PostgreSQL Connections

#MySQL Connection
try:
    mysql_db = mysql.connector.connect(host="165.22.220.96",user="user1",password="Admin_123",db="eBooks2go")
    mycursor = mysql_db.cursor()
    print("MySQL Connection Established")
except mysql.connector.Error as e:
    print("Unable to Connect: ",format(e))

#PostgreSQL Connection
try:
    pg_db = psycopg2.connect(host="165.22.220.96",user="praveen",password="Admin123",database="staging")
    pg_db.autocommit = True
    pgcursor = pg_db.cursor()
    print("PostgreSQL Connection Established")
except psycopg2.OperationalError as e:
    print("Unable to Connect: ",format(e))
    

new_records_source = 0
new_records_target = 0
count_failed = 0

#Selects data from table
slt_cmd ="Select * from eBooks2go.BLC_SALESREPORTS where SALES_ID > " + str(last_value)
mycursor.execute(slt_cmd)

#Extracting column names 
col_names_lst = [i[0] for i in mycursor.description] 
col_names = ', '.join(col_names_lst)+', Created_Date'+', Updated_Date'

#Inserting data from MySQL into PostgreSQL
placeholders = ', '.join(['%s'] * (len(col_names_lst)+2) ) 

#Inserting data into PostgreSQL
insrt_cmd = "INSERT INTO stg_SALESREPORTS("+col_names+") " \
            "VALUES ( %s )" % (placeholders)
#     print(insrt_cmd)

for row in mycursor:
    try:
        new_records_source = mycursor.rowcount
        pgcursor.execute(insrt_cmd,row+tuple([time.strftime("%Y/%m/%d %H:%M")])+tuple([time.strftime("%Y/%m/%d %H:%M")]))
        new_records_target += 1
    except:
        print(e)
        count_failed += 1 
    
#Log_Audit
mycursor.execute('SELECT count(sales_id) FROM eBooks2go.BLC_SALESREPORTS')
Total_Records_from_source = ','.join(map(str,[str(x[0]) for x in mycursor.fetchall()]))

pgcursor.execute('SELECT count(sales_id) FROM stg_SALESREPORTS')
Total_Records_from_target = ','.join(map(str,[str(x[0]) for x in pgcursor.fetchall()]))

insrt_log = "INSERT INTO LOG_Audit (Phase, Source_Table_Name, Target_Table_Name, \
Total_Records_Source_Table, Total_Records_Target_Table, New_Records_Source, \
New_Records_Target,Status, Remarks, Execution_time) Values ('LtoS','eBooks2go.BLC_SALESREPORTS','stg_SALESREPORTS'," + str(Total_Records_from_source) + "," + str(Total_Records_from_target) + "," + str(new_records_source) + "," + str(new_records_target) + ",'Completed','" + str(count_failed) + " Records Failed.Incremental Data Update','" + str(round(time.time() - start_time,2))+" seconds')"
pgcursor.execute(insrt_log)
                       
mysql_db.close()
pg_db.close()

