#!/usr/bin/env python
# coding: utf-8


import psycopg2
import time
import numpy as np


start_time = time.time()


#PostgreSQL Connection
try:
    pg_db = psycopg2.connect(host="165.22.220.96",user="praveen",password="Admin123",database="staging")
    pg_db.autocommit = True
    pgcursor = pg_db.cursor()
    print("PostgreSQL Connection Established")
except psycopg2.OperationalError as e:
    print("Unable to Connect: ",format(e))

# PostgreSQL Connection 1
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
    
#PostgreSQL Connection 3
try:
    pg_db3 = psycopg2.connect(host="165.22.220.96",user="praveen",password="Admin123",database="staging")
    pg_db3.autocommit = True
    pgcursor3 = pg_db3.cursor()
    print("PostgreSQL Connection Established")
except psycopg2.OperationalError as e:
    print("Unable to Connect: ",format(e))

#PostgreSQL Connection 4
try:
    pg_db4 = psycopg2.connect(host="165.22.220.96",user="praveen",password="Admin123",database="staging")
    pg_db4.autocommit = True
    pgcursor4 = pg_db4.cursor()
    print("PostgreSQL Connection Established")
except psycopg2.OperationalError as e:
    print("Unable to Connect: ",format(e))


new_records_target = 0
count_failed = 0

slt_cmd = "SELECT master_customer_id, email_address FROM master_customer_test"
pgcursor.execute(slt_cmd)

for i in pgcursor:
    try:
    #   establish all pgcursors connecting to each individual source table

        pgcursor1.execute("SELECT email_address from stg_crm_customer where email_address = '"+i[1]+"'")
        pgcursor3.execute("SELECT email_address from stg_blc_customer where email_address = '"+i[1]+"'")
        pgcursor4.execute("SELECT email from stg_bpm_client_info where email = '"+i[1]+"'")
    #         ------------------------------------------------------------------------------------------------------
        for x in pgcursor1:
            if i[1] == x[0]:
                pgcursor2.execute("INSERT INTO dw_ref_source_dim (master_customer_id,source_id,created_date, updated_date) values ("+str(i[0])+", 3,current_timestamp,current_timestamp)")
            break
        for y in pgcursor3:
            if i[1] == y[0]:
                pgcursor2.execute("INSERT INTO dw_ref_source_dim (master_customer_id, source_id,created_date, updated_date) values ("+str(i[0])+", 1,current_timestamp,current_timestamp)")
            break
        for z in pgcursor4:
            if i[1] == z[0]:
                pgcursor2.execute("INSERT INTO dw_ref_source_dim (master_customer_id, source_id,created_date, updated_date) values ("+str(i[0])+", 2,current_timestamp,current_timestamp)")
            break  

    except Exception as e:
        count_failed += 1


#Log_Audit
Total_Records_from_source = 0
new_records_source = Total_Records_from_source

pgcursor1.execute('SELECT count(source_id) FROM dw_ref_source_dim')
Total_Records_from_target = ','.join(map(str,[str(x[0]) for x in pgcursor1.fetchall()]))

new_records_target = Total_Records_from_target

insrt_log = "INSERT INTO LOG_audit (Phase, Source_Table_Name, Target_Table_Name, \
Total_Records_Source_Table, Total_Records_Target_Table, New_Records_Source, \
New_Records_Target,Status, Remarks, Execution_time) Values ('StoDW','None','dw_ref_source_dim'," + str(Total_Records_from_source) + "," + str(Total_Records_from_target) + ","+ str(new_records_source) + "," + str(new_records_target) + ",'Completed','" + str(count_failed) + " Records Failed.Historical Update','" + str(round(time.time() - start_time,2))+" seconds')"
pgcursor.execute(insrt_log)

pg_db.close()
pg_db1.close()
pg_db2.close()
pg_db3.close()
pg_db4.close()
