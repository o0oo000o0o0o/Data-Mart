#!/usr/bin/env python
# coding: utf-8


import mysql.connector
import psycopg2
import pandas as pd
import time


start_time = time.time()


#PostgreSQL Connection
try:
    pg_db = psycopg2.connect(host="165.22.220.96",user="praveen",password="Admin123",database="staging")
    pg_db.autocommit = True
    pgcursor = pg_db.cursor()
    print("PostgreSQL Connection Established")
except psycopg2.OperationalError as e:
    print("Unable to Connect: ",format(e))

#PostgreSQL Connection 2
try:
    pg_db1 = psycopg2.connect(host="165.22.220.96",user="praveen",password="Admin123",database="staging")
    pg_db1.autocommit = True
    pgcursor1 = pg_db1.cursor()
    print("PostgreSQL Connection Established")
except psycopg2.OperationalError as e:
    print("Unable to Connect: ",format(e))

new_records_source = 0
new_records_target = 0
count_failed = 0

#Selects data from table
slt_cmd ="Select store_id,store_name,store_royalty,Created_Date,Updated_Date from stg_stores"
pgcursor.execute(slt_cmd)

#Extracting column names 
col_names_lst = [i[0] for i in pgcursor.description] 
col_names = ', '.join(col_names_lst)
# print(col_names)

#Inserting data from PostgreSQL STG into PostgreSQL DW
placeholders = ', '.join(['%s'] * (len(col_names_lst)) ) 

#Inserting data into PostgreSQL DW
insrt_cmd = "INSERT INTO DW_stores_dim("+col_names+") " \
            "VALUES ( %s )" % (placeholders)
# print(insrt_cmd)

for row in pgcursor:
    try:
        new_records_source = pgcursor.rowcount
        pgcursor1.execute(insrt_cmd,row)
        
        new_records_target += 1
        
    except Exception as e:
        count_failed += 1


#Log_Audit
pgcursor1.execute('SELECT count(store_id) FROM stg_stores')
Total_Records_from_source = ','.join(map(str,[str(x[0]) for x in pgcursor1.fetchall()]))

pgcursor.execute('SELECT count(store_id) FROM DW_stores_dim')
Total_Records_from_target = ','.join(map(str,[str(x[0]) for x in pgcursor.fetchall()]))

insrt_log = "INSERT INTO LOG_audit (Phase, Source_Table_Name, Target_Table_Name, \
Total_Records_Source_Table, Total_Records_Target_Table, New_Records_Source, \
New_Records_Target,Status, Remarks, Execution_time) Values ('StoDW','stg_stores','DW_stores_dim'," + str(Total_Records_from_source) + "," + str(Total_Records_from_target) +","+ str(new_records_source) + "," + str(new_records_target) + ",'Completed','" + str(count_failed) + " Records Failed.Incremental Update','" + str(round(time.time() - start_time,2))+" seconds')"
pgcursor.execute(insrt_log)
    
    
pg_db.close()
pg_db1.close()

