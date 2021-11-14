#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import psycopg2
import pandas as pd
import time

start_time = time.time()


# In[ ]:


### Establishing MySQL and PostgreSQL Connections

#PostgreSQL Connection
try:
    pg_db = psycopg2.connect(host="165.22.220.96",user="praveen",password="Admin123",database="staging")
    pg_db.autocommit = True
    pgcursor = pg_db.cursor()
    print("PostgreSQL Connection Established")
except psycopg2.OperationalError as e:
    print("Unable to Connect: ",format(e))

#PostgreSQL Connection
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
    


# In[ ]:


new_records_source = 0
new_records_target = 0
count_failed = 0

slt_mc = "select master_customer_id, email_address from dw_master_customer_dim"
pgcursor.execute(slt_mc)

for i in pgcursor:
#     new_records_source = pgcursor.rowcount
    print(i)
    try:
        slt_blc = "select publisher_name,email_address from stg_blc_customer where email_address = '"+i[1]+"'                     and (role_id = 1 and address_line1 is not null) and publisher_name is not null and publisher_name != '' "

        #Selecting BLC Publisher_Names
        pgcursor1.execute(slt_blc)

        for x in pgcursor1:
            print(x)
            if i[1] == x[1]:
                if x[0] != None:
                    z = x[0].replace("'", "''")
                    print("insert into dw_company_name_dim (master_customer_id,company_name,created_date,updated_date)                    values ("+str(i[0])+", '"+str(z)+"', current_timestamp, current_timestamp)")
#                     pgcursor2.execute("insert into dw_company_name_dim (master_customer_id,company_name,created_date,updated_date)\
#                     values ("+str(i[0])+", '"+str(z)+"', current_timestamp, current_timestamp)")
    #             else:
    #                 pgcursor2.execute("insert into dw_company_name_dim (master_customer_id,company_name,created_date,updated_date)\
    #                 values ("+str(i[0])+", '"+str(x[0])+"', current_timestamp, current_timestamp)")

        slt_bpm = """select company_name,email from stg_bpm_client_info where email = '"+i[1]+"' 
                and company_name is not null and company_name != '' 
                group by company_name,email"""

        pgcursor1.execute(slt_bpm)

        for x in pgcursor1:
            if i[1] == x[1]:
                if x[0] != None:
                    z = x[0].replace("'", "''")
                    print("insert into dw_company_name_dim (master_customer_id,company_name,created_date,updated_date)                    values ("+str(i[0])+", '"+str(z)+"', current_timestamp, current_timestamp)")
#                     pgcursor2.execute("insert into dw_company_name_dim (master_customer_id,company_name,created_date,updated_date)\
#                     values ("+str(i[0])+", '"+str(z)+"', current_timestamp, current_timestamp)")
    #             else:
    #                 values ("+str(i[0])+", '"+str(x[0])+"', current_timestamp, current_timestamp)")

        new_records_target += 1
        
    except Exception as e:
        count_failed += 1 


# In[ ]:


# #Log_Audit
# Total_Records_from_target = 0

# pgcursor1.execute('SELECT count(company_name_id) FROM dw_company_name_dim')
# Total_Records_from_source = ','.join(map(str,[str(x[0]) for x in pgcursor1.fetchall()]))

# insrt_log = "INSERT INTO LOG_audit (Phase, Source_Table_Name, Target_Table_Name, \
# Total_Records_Source_Table, Total_Records_Target_Table, New_Records_Source, \
# New_Records_Target,Status, Remarks, Execution_time) Values ('StoDW','None','dw_company_name_dim'," + str(Total_Records_from_source) + "," + str(Total_Records_from_target) + ","+ str(new_records_source) + "," + str(new_records_target) + ",'Completed','" + str(count_failed) + " Records Failed.Incremental Update','" + str(round(time.time() - start_time,2))+" seconds')"
# pgcursor2.execute(insrt_log)

pg_db.close()
pg_db1.close()
pg_db2.close()

