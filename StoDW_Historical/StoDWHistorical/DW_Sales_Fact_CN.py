#!/usr/bin/env python
# coding: utf-8


import mysql.connector
import psycopg2
import pandas as pd
import time

start_time = time.time()

### Establishing MySQL and PostgreSQL Connections

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

#PostgreSQL Connection 2
try:
    pg_db2 = psycopg2.connect(host="165.22.220.96",user="praveen",password="Admin123",database="staging")
    pg_db2.autocommit = True
    pgcursor2 = pg_db2.cursor()
    print("PostgreSQL Connection Established")
except psycopg2.OperationalError as e:
    print("Unable to Connect: ",format(e))
    

count_success = 0
Total_Records_from_source = 0
count_failed_emails = 0

pgcursor.execute("""select C.customer_id, C.publisher_name from dw_sales_fact AS S, stg_blc_customer AS C 
                WHERE S.CUSTOMER_ID = C.CUSTOMER_ID""")
for x in pgcursor:
    try:
        Total_Records_from_source = pgcursor.rowcount

        if x[1] is None:
#             print(x[1])
        else:
            pgcursor1.execute("select company_name_id from dw_company_name_dim \
                        WHERE company_name = '"+ str(x[1].replace("'", "''"))+"'")
            for y in pgcursor1:
                pgcursor2.execute("update dw_sales_fact \
                                    set company_name_id = "+ str(y[0])+ " where customer_id = "+str(x[0]))
        count_success += 1
    
    except Exception as e:
        print(e)
        count_failed_emails += 1
        
insrt_log = "INSERT INTO LOG_DIM (Phase, Source_Table_Name, Target_Table_Name, Total_Records_Source_Table,Total_Records_Target_Table, Status, Remarks, Execution_time) Values ('StoDW','dw_company_name_dim','dw_sales_fact'," +str(Total_Records_from_source)+", "+str(count_success)+",'Completed','" +str(count_failed_emails)+" Failed','"+str(round(time.time() - start_time,2))+" seconds')"
pgcursor.execute(insrt_log)
    
pg_db.close()
pg_db1.close()
pg_db2.close()




