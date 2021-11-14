#!/usr/bin/env python
# coding: utf-8

import mysql.connector
import psycopg2
import pandas as pd
import time
import sqlalchemy
from sqlalchemy import create_engine, inspect

start_time = time.time()

postgres_engine = create_engine('postgresql://praveen:Admin123@165.22.220.96:5432/staging')

last_value = pd.read_sql('SELECT max(distribution_orderinfo_id) FROM DW_Distribution_Order_Info_DIM', con=postgres_engine).iloc[0,0]
last_value

#PostgreSQL Connection
try:
    pg_db = psycopg2.connect(host="165.22.220.96",user="praveen",password="Admin123",database="staging")
    pg_db.autocommit = True
    pgcursor = pg_db.cursor()
    print("PostgreSQL Connection Established")
except psycopg2.OperationalError as e:
    print("Unable to Connect: ",format(e))

#PostgreSQL Connection 1
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
slt_cmd ="Select Distribution_OrderInfo_ID,ISBN,ARCHIVED,EBOOK,LIST_PRICE,ORDER_STATUS,ORDER_STATUS_DATE, \
TITLE,DISTRIBUTION_ORDER_INSTORES_ID,DISTRIBUTION_METADATA_ID,ID,ORDER_DATE,CCID,FORMAT, \
UPDATED_STATUS,CREATED_DATE,UPDATED_DATE from stg_distribution_orderinfo where DISTRIBUTION_ORDERINFO_ID > " + str(last_value)
pgcursor.execute(slt_cmd)

#Extracting column names 
col_names_lst = [i[0] for i in pgcursor.description] 
col_names = ', '.join(col_names_lst)

#Inserting data from PostgreSQL STG into PostgreSQL DW
placeholders = ', '.join(['%s'] * (len(col_names_lst)) ) 

#Inserting data into PostgreSQL DW
insrt_cmd = "INSERT INTO DW_Distribution_Order_Info_DIM(Distribution_OrderInfo_ID,ISBN,ARCHIVED,EBOOK,LIST_PRICE,ORDER_STATUS,ORDER_STATUS_DATE, \
TITLE,DISTRIBUTION_ORDER_INSTORES_ID,DISTRIBUTION_METADATA_ID,CUSTOMER_ID,ORDER_DATE,CCID,FORMAT, \
UPDATED_STATUS,CREATED_DATE,UPDATED_DATE) " \
            "VALUES ( %s )" % (placeholders)

for row in pgcursor:
    try:
        new_records_source = pgcursor.rowcount
        pgcursor1.execute(insrt_cmd,row)
        new_records_target += 1
    
    except Exception as e:
        print(e)
        count_failed_emails += 1 
        
#Log_Audit
pgcursor1.execute('SELECT count(distribution_orderinfo_id) FROM stg_DISTRIBUTION_ORDERINFO')
Total_Records_from_source = ','.join(map(str,[str(x[0]) for x in pgcursor1.fetchall()]))

pgcursor.execute('SELECT count(distribution_orderinfo_id) FROM DW_Distribution_Order_Info_DIM')
Total_Records_from_target = ','.join(map(str,[str(x[0]) for x in pgcursor.fetchall()]))

insrt_log = "INSERT INTO LOG_audit (Phase, Source_Table_Name, Target_Table_Name, \
Total_Records_Source_Table, Total_Records_Target_Table, New_Records_Source, \
New_Records_Target,Status, Remarks, Execution_time) Values ('StoDW','stg_DISTRIBUTION_ORDERINFO','DW_Distribution_Order_Info_DIM'," + str(Total_Records_from_source) + "," + str(Total_Records_from_target) + "," + str(new_records_source) + "," + str(new_records_target) + ",'Completed','" + str(count_failed) + " Records Failed. Incremental Update','" + str(round(time.time() - start_time,2))+" seconds')"
pgcursor.execute(insrt_log)
    
pg_db.close()
pg_db1.close()