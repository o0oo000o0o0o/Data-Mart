#!/usr/bin/env python
# coding: utf-8

import psycopg2
import pandas as pd
import time
import sqlalchemy
from sqlalchemy import create_engine, inspect

start_time = time.time()

### Establishing PostgreSQL Connections

#PostgreSQL Connection
try:
    postgres_engine = create_engine('postgresql://praveen:Admin123@165.22.220.96:5432/staging')
    print("PostgreSQL Connection Established")
except psycopg2.OperationalError as e:
    print("Unable to Connect: ",format(e))
    
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
    
new_records_source = 0
new_records_target = 0
count_failed = 0

#Selects Sales data from table
slt_sales ="Select SALES_ID,DATE_OF_SALE,COUNTRY_OF_SALE,CUSTOMER_CURRENCY,CUSTOMER_SHARE,DELIVARY_COST,DISCOUNT, EBOOKS_SHARE,FINAL_PRICE,NET_UNITS,QUANTITY_OF_SALE,STORE_PRICE,UNITS_RETURNED,DISTRIBUTION_ORDERINFO_ID, CUSTOMER_ID,STORE_ID,FORMAT from stg_SALESREPORTS where customer_id != 0 and distribution_orderinfo_id != 0"
pgcursor.execute(slt_sales)

print("Selected Data from Staging Area")

#Extracting column names 
col_names_lst = [i[0] for i in pgcursor.description] 
col_names = ', '.join(col_names_lst)+', Created_Date'+', Updated_Date'
# print(col_names)

#Inserting data from PostgreSQL STG into PostgreSQL DW
placeholders = ', '.join(['%s'] * (len(col_names_lst)+2)) 

#Inserting data into PostgreSQL DW
insrt_sales = "INSERT INTO dw_sales_fact("+col_names+") " "VALUES ( %s )" % (placeholders)

for row in pgcursor:
    try:
        new_records_source = pgcursor.rowcount
        pgcursor1.execute(insrt_sales,row+tuple([time.strftime("%Y/%m/%d %H:%M")])+tuple([time.strftime("%Y/%m/%d %H:%M")]))

        new_records_target += 1
    
    except Exception as e:
        count_failed += 1

print("Inserted Data into Data Warehouse")

# Updating Format_id into Sales_Fact
update_format = """UPDATE dw_sales_fact
                SET format_id = fd.format_id
                from (SELECT format_id, format_type 
                       FROM dw_format_dim) fd
                WHERE format = fd.format_type"""
pgcursor.execute(update_format)

print("Updated Format_ids into Sales_Fact")

#Updating Master_customer_id into Sales_fact
slt_email = """select DISTINCT C.EMAIL_ADDRESS,C.customer_id,C.publisher_name
                        from dw_sales_fact AS S, stg_blc_customer AS C
                        WHERE S.CUSTOMER_ID = C.CUSTOMER_ID"""

slt_mc = """select master_customer_id
                    from master_customer_test
                    where email_address = '"""

upd_sales = """UPDATE dw_sales_fact
                SET master_customer_id = """

pgcursor.execute(slt_email) 
for email in pgcursor:
    try:
    #     print(row)
        slt_mcid = slt_mc+str(email[0])+"'"
        pgcursor1.execute(slt_mcid)
        for mcid in pgcursor1:
    #         print(data)
            upd_sales_fact = upd_sales+str(mcid[0])+" where customer_id = "+str(email[1])+";"
            pgcursor2.execute(upd_sales_fact)
            
    except Exception as e:
        print(e)

print("Updated master_customer_ids into Sales_Fact")

#Updating Company_Name_id into Sales_fact

try:
    pgcursor1.execute("select master_customer_id,company_name_id from dw_company_name_dim)
    for cn in pgcursor1: 
        pgcursor2.execute("update dw_sales_fact set company_name_id = "+ str(cn[1])+ " where master_customer_id = "+ str(cn[0]))
except Exception as e:
    print(e)

#Log_Audit
Total_Records_from_source = pd.read_sql('SELECT count(sales_id) FROM stg_SALESREPORTS', con=postgres_engine).iloc[0,0]

Total_Records_from_target = pd.read_sql('SELECT count(sales_id) FROM dw_sales_fact', con=postgres_engine).iloc[0,0]

insrt_log = "INSERT INTO LOG_audit (Phase, Source_Table_Name, Target_Table_Name, \
Total_Records_Source_Table, Total_Records_Target_Table, New_Records_Source, \
New_Records_Target,Status, Remarks, Execution_time) Values ('StoDW','stg_SALESREPORTS',\
'dw_sales_fact'," +str(Total_Records_from_source)+","+str(Total_Records_from_target)+","+str(new_records_source)\
+","+str(new_records_target)+",'Completed','" +str(count_failed)+" Records Failed. Updated Format_id, Master_customer_id, Comapny_name_id. Incremental Update','"\
+str(round(time.time() - start_time,2))+" seconds')"
pgcursor.execute(insrt_log)

print("Done!")

pg_db.close()
pg_db1.close()
pg_db2.close()

print("--- %s seconds ---" % (time.time() - start_time))
