import psycopg2
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, inspect
import time


start_time = time.time()
postgres_engine = create_engine('postgresql://praveen:Admin123@165.22.220.96:5432/staging')

new_records_source = 0
new_records_target = 0
count_failed = 0

try:
    # Import Master_customer_id and Company_Names from source tables [blc and bpm]
    blc_mc = pd.read_sql("""select distinct master_customer_id,publisher_name
                            from dw_master_customer_dim, stg_blc_customer
                            where dw_master_customer_dim.email_address = stg_blc_customer.email_address and (publisher_name is not null 
                            and publisher_name != '') and (stg_blc_customer.role_id =1 and stg_blc_customer.address_line1 is not null)""", con=postgres_engine)

    bpm_mc = pd.read_sql("""select distinct master_customer_id,company_name
                    from dw_master_customer_dim, stg_bpm_client_info
                    where dw_master_customer_dim.email_address = stg_bpm_client_info.email and company_name is not null and company_name != '' """, con=postgres_engine)

    # Bring BLC and BPM Data to a Unified format and Combine them
    blc_mc = blc_mc.rename(columns={"publisher_name": "company_name"})
    total_data = blc_mc.append(bpm_mc)
    
    #Remove any possible duplicates after
    total_data =total_data[~total_data.duplicated()]
    print(total_data)

    total_data.to_sql(name='dw_company_name_dim', con=postgres_engine, if_exists='append', index=False, method='multi')
    
    new_records_target += total_data.shape[0]
    
except Exception as e:
    count_failed += 1


#Log_Audit

#PostgreSQL Connection
try:
    pg_db = psycopg2.connect(host="165.22.220.96",user="praveen",password="Admin123",database="staging")
    pg_db.autocommit = True
    pgcursor = pg_db.cursor()
    print("PostgreSQL Connection Established")
except psycopg2.OperationalError as e:
    print("Unable to Connect: ",format(e))

Total_Records_from_source = 0

pgcursor.execute('SELECT count(company_name_id) FROM dw_company_name_dim')
Total_Records_from_target = ','.join(map(str,[str(x[0]) for x in pgcursor.fetchall()]))

insrt_log = "INSERT INTO LOG_audit (Phase, Source_Table_Name, Target_Table_Name,Total_Records_Source_Table, Total_Records_Target_Table, New_Records_Source,New_Records_Target,Status, Remarks, Execution_time) Values ('StoDW','None','dw_company_name_dim'," + str(Total_Records_from_source) + "," + str(Total_Records_from_target) + ","+ str(new_records_source) + "," + str(new_records_target) + ",'Completed','" + str(count_failed) + " Records Failed.Historical Update','" + str(round(time.time() - start_time,2))+" seconds')"
pgcursor.execute(insrt_log)