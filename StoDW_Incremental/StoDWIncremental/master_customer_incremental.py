#!/usr/bin/env python
# coding: utf-8

import psycopg2
import sqlalchemy
from sqlalchemy import create_engine, inspect
import pandas as pd
import time
import re
import numpy as np

start_time = time.time()


postgres_engine = create_engine('postgresql://praveen:Admin123@165.22.220.96:5432/staging')

#PostgreSQL Connection
try:
    pg_db = psycopg2.connect(host="165.22.220.96",user="praveen",password="Admin123",database="staging")
    pg_db.autocommit = True
    pgcursor = pg_db.cursor()
    print("PostgreSQL Connection Established")
except psycopg2.OperationalError as e:
    print("Unable to Connect: ",format(e))

# Get last updated date
last_updated = ','.join(map(str,[str(pd.read_sql('SELECT max(updated_date) FROM dw_master_customer_dim', con=postgres_engine).iloc[0,0])]))
print(last_updated)

# Get counts for Audit before new records are added into the table
pgcursor.execute('SELECT count(master_customer_id) FROM dw_master_customer_dim')
Total_Records_from_source = ','.join(map(str,[str(x[0]) for x in pgcursor.fetchall()]))
print('Total_Records_from_source:',Total_Records_from_source)


#Select only those records from staging that were added after the last update
stg_crm = pd.read_sql("SELECT * FROM stg_crm_customer where updated_date > '"+ last_updated+"'", con=postgres_engine)
stg_bpm = pd.read_sql("SELECT * FROM stg_bpm_client_info where updated_date > '"+ last_updated+"'", con=postgres_engine)
stg_blc = pd.read_sql("SELECT * FROM stg_blc_customer where updated_date > '"+ last_updated+"'", con=postgres_engine)

### CRM
stg_crm["role_id"] = 1
stg_crm = stg_crm.rename(columns={"locality": "city", "region": "state", "country_code": "country", 
                          "given_name": "first_name", "phonenumber": "phone", "address1": "address_line1", 
                          "address2": "address_line2", "zip_code": "zip"})
stg_crm = stg_crm[['customer_id', 'email_address', 'date_created', 'first_name', 'address_line1', 'address_line2', 'city', 'state', 'country', 'phone', 'role_id']]
stg_crm.shape

#Validating Email_addresses Using REGEX
crm = stg_crm[stg_crm.email_address.str.contains('[\w\.-]+@[\w\.-]+', regex= True, na=False)]
crm.shape

new_master = crm

print('crm new master:',new_master.shape)

### BPM
stg_bpm["role_id"] = 1
stg_bpm = stg_bpm.rename(columns={"company_name": "publisher_name", "email": "email_address", "address": "address_line1", "date": "date_created", "clientid": "customer_id"})
stg_bpm = stg_bpm[['address_line1', 'city', 'state','country', 'zip', 'phone', 'fax', 
           'email_address','date_created','role_id']]
stg_bpm.shape

#Validating Email_addresses Using REGEX
bpm = stg_bpm[stg_bpm.email_address.str.contains('[\w\.-]+@[\w\.-]+', regex= True, na=False)]
bpm.shape

new_master = new_master.append(bpm)
print('bpm new master:',new_master.shape)


### BLC
stg_blc = stg_blc[['date_created','deactivated', 'email_address','first_name', 'last_name', 'password', 
           'user_name', 'address_line1','city', 'phone', 'zip','country_name', 'state_name', 'role_id',
           'is_publisher','website', 'is_partner','lead_owner','lead_source', 'order_type']]
stg_blc = stg_blc.rename(columns={"country_name": "country", "state_name": "state"})
stg_blc = stg_blc.loc[(stg_blc["role_id"] == 1) & pd.notnull(stg_blc["address_line1"])]
stg_blc['order_type'] = stg_blc['order_type'].fillna('1')

stg_blc.shape

#Validating Email_addresses Using REGEX
blc = stg_blc[stg_blc.email_address.str.contains('[\w\.-]+@[\w\.-]+', regex= True, na=False)]
blc.shape

new_master = new_master.append(blc)
print('blc new master:', new_master)

new_master.columns


### MASTER CUSTOMER TABLE AND GOLDEN RECORD CREATION
new_master = new_master.drop(['customer_id'], 1)
print(new_master.columns)

new_master['email_address'] = new_master['email_address'].str.lower()
new_master = new_master.replace({"\'":"''" }, regex=True)
new_master = new_master.replace(r'^([\s])$|^(?![\s\S])$', np.nan, regex=True)
new_master['date_created'] = pd.to_datetime(new_master['date_created'])
new_master['email_address'] = [i.strip() for i in new_master['email_address']]
print(new_master.shape)
new_master.columns

# Switch-Case Implementation for Attribute Specific Implementation
def first_name(dataframe):
    column_specific = dataframe[['date_created','first_name']]                        [(dataframe['first_name'].isna()==False)]    
    return column_specific
 
def last_name(dataframe):
    column_specific = dataframe[['date_created','last_name']]                        [(dataframe['last_name'].isna()==False)]
    return column_specific

def address_line1(dataframe):
    column_specific = dataframe[['date_created','address_line1','address_line2','city','state','country','zip']]                    [(dataframe['address_line1'].isna()==False)]
    return column_specific
 
def phone(dataframe):
    column_specific = dataframe[['date_created','phone']]                    [(dataframe['phone'].isna()==False)]
    return column_specific

def fax(dataframe):
    column_specific = dataframe[['date_created','fax']]                    [(dataframe['fax'].isna()==False)]
    return column_specific

def website(dataframe):
    column_specific = dataframe[['date_created','website']]                    [(dataframe['website'].isna()==False)]
    return column_specific

def is_partner(dataframe):
    column_specific = dataframe[['date_created','is_partner']]                    [(dataframe['is_partner'].isna()==False)]
    return column_specific

def lead_owner(dataframe):
    column_specific = dataframe[['date_created','lead_owner']]                    [(dataframe['lead_owner'].isna()==False)]
    return column_specific

def lead_source(dataframe):
    column_specific = dataframe[['date_created','lead_source']]                    [(dataframe['lead_source'].isna()==False)]
    return column_specific

def order_type(dataframe):
    column_specific = dataframe[['date_created','order_type']]                    [(dataframe['order_type'].isna()==False)]
    return column_specific

switcher = {
        0: first_name,
        1: last_name,
        2: address_line1,
        3: phone,
        4: fax,
        5: website,
        6: is_partner,
        7: lead_owner,
        8: lead_source,
        9: order_type
    }
 
def sub_df(argument,dataframe):
    # Get the function from switcher dictionary
    func = switcher.get(argument)
    # Execute the function
    return func(dataframe)


print('got the switch cases')


def golden_records(i,email_specific):
#     print(i)

    data = {}

    if i == None:
        print("Its None")

    elif email_specific.shape[0] == 1:
        data = email_specific.to_dict('record')[0]
#         print(data)

    else:
        data['email_address'] = i

        for num in range(0,10):
#             print(num)
            column_specific = sub_df(num,email_specific)
#             display('column_specific:',column_specific)

            if column_specific.shape[0] == 0:
                for attribute in column_specific.columns[1:]:
                    data[attribute]= np.nan

            elif column_specific.shape[0] == 1:
                val = column_specific.iloc[:,1:].to_dict('record')
#                 display('Val',val)
                data.update(val[0])

            else:
                if column_specific['date_created'].isnull().values.all() == True:
                    last_value = column_specific.iloc[:,1:].iloc[-1].to_dict('record')
#                     print('last_value:','\n',last_value,'\n')
                    data.update(last_value[0])

                else:
                    date_created = column_specific['date_created'].max()
#                     print('date_created:',date_created)

                    recent_record = column_specific[column_specific['date_created']==date_created].iloc[:,1:].to_dict('records')
#                     display('recent_record',recent_record[0])
                    data.update(recent_record[0])

#     print('Data:','\n',data)
#     print('------------')
    # Final dataframe to import into postgres
    data_df = pd.DataFrame(data.items())
    data_df = data_df.set_index(0).T
    data_df = data_df.where(pd.notnull(data_df), None)
    return data_df

new_records_target = 0
count_failed = 0
updated = 0

for i in new_master['email_address'].unique():
#     print(i)
    
    master_data = pd.read_sql("SELECT * FROM dw_master_customer_dim where email_address = '"+i+"'", con=postgres_engine)
    master_data = master_data.loc[:, master_data.columns != 'master_customer_id']
#     print('master_data',master_data)
    
    temp_df = new_master[new_master['email_address'] == i]
    
    try:
    
        if master_data.shape[0] == 0:
            data_df = golden_records(i,temp_df)
            new_records_target += 1

        else:
            master_data = master_data.append(temp_df)
            data_df = golden_records(i,master_data)
            pgcursor.execute("Delete FROM dw_master_customer_dim where email_address = '"+ i +"'")
            
            updated += 1

    except Exception as e:
        print(e)
#         failed_emails[i]=e
        count_failed += 1 

    else:
#         print('data_df:','\n')
#         print(data_df)
        data_df.to_sql(name='dw_master_customer_dim', con=postgres_engine, if_exists='append', index=False, method='multi')

# Log_Audit
new_records_source = len(new_master['email_address'].unique())
new_records_source

pgcursor.execute('SELECT count(master_customer_id) FROM dw_master_customer_dim')
Total_Records_from_target = ','.join(map(str,[str(x[0]) for x in pgcursor.fetchall()]))

insrt_log = "INSERT INTO LOG_audit (Phase, Source_Table_Name, Target_Table_Name, \
Total_Records_Source_Table, Total_Records_Target_Table, New_Records_Source, \
New_Records_Target,Status, Remarks, Execution_time) Values ('StoDW','3 tables [stg-blc-crm-bpm-customer]', 'dw_master_customer_dim'," + str(Total_Records_from_source) + "," + str(Total_Records_from_target) + "," + str(new_records_source) + "," + str(new_records_target) + ",'Completed','" + str(count_failed) + " Records Failed.Incremental Update."+str(updated)+" Records updated.','" + str(round(time.time() - start_time,2))+" seconds')"
print(insrt_log)
pgcursor.execute(insrt_log)
