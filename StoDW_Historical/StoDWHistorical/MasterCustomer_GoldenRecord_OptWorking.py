#!/usr/bin/env python
# coding: utf-8


import psycopg2
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
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


# In[ ]:


stg_crm = pd.read_sql('SELECT * FROM stg_crm_customer', con=postgres_engine)
stg_bpm = pd.read_sql('SELECT * FROM stg_bpm_client_info', con=postgres_engine)
stg_blc = pd.read_sql('SELECT * FROM stg_blc_customer', con=postgres_engine)
master = pd.read_sql('SELECT * FROM master_customer_test', con=postgres_engine)
master.columns

### CRM
stg_crm["role_id"] = 1
stg_crm = stg_crm.rename(columns={"locality": "city", "region": "state", "country_code": "country", 
                          "given_name": "first_name", "phonenumber": "phone", "address1": "address_line1", 
                          "address2": "address_line2", "zip_code": "zip"})
stg_crm = stg_crm[['customer_id', 'email_address', 'date_created', 'first_name','country', 'phone', 'email_status', 
           'created_date', 'updated_date','role_id']]
stg_crm.shape

#Validating Email_addresses Using REGEX
crm = stg_crm[stg_crm.email_address.str.contains('[\w\.-]+@[\w\.-]+', regex= True, na=False)]
crm.shape

new_master = master.append(crm)

### BPM

stg_bpm["role_id"] = 1
stg_bpm = stg_bpm.rename(columns={"company_name": "publisher_name", "email": "email_address", "address": "address_line1", "date": "date_created", "clientid": "customer_id"})
stg_bpm = stg_bpm[['customer_id', 'publisher_name', 'address_line1', 'city', 'state','country', 'zip', 'phone', 'fax', 
           'email_address','date_created', 'created_date', 'updated_date','role_id']]
stg_bpm

#Validating Email_addresses Using REGEX
bpm = stg_bpm[stg_bpm.email_address.str.contains('[\w\.-]+@[\w\.-]+', regex= True, na=False)]
bpm.shape

new_master = new_master.append(bpm)
new_master.shape

### BLC

stg_blc = stg_blc[['customer_id', 'date_created','deactivated', 'email_address','first_name', 'last_name', 'password', 
           'user_name', 'address_line1', 'city', 'phone', 'zip','country_name', 'state_name', 'role_id',
           'is_publisher', 'publisher_name', 'website', 'is_partner', 'date_of_sale','lead_owner', 
           'lead_source', 'order_type', 'created_date','updated_date']]
stg_blc = stg_blc.rename(columns={"country_name": "country", "state_name": "state"})
stg_blc = stg_blc.loc[(stg_blc["role_id"] == 1) & pd.notnull(stg_blc["address_line1"])]
stg_blc['order_type'] = stg_blc['order_type'].fillna('1')

stg_blc.shape

#Validating Email_addresses Using REGEX
blc = stg_blc[stg_blc.email_address.str.contains('[\w\.-]+@[\w\.-]+', regex= True, na=False)]
blc.shape

new_master = new_master.append(blc)
new_master.shape


### MASTER CUSTOMER TABLE AND GOLDEN RECORD CREATION

new_master = new_master.drop(['master_customer_id','email_status','customer_id','created_date','updated_date','date_of_sale','publisher_name'], 1)
new_master.columns

new_master['email_address'] = new_master['email_address'].str.lower()
new_master = new_master.replace({"\'":"''" }, regex=True)
new_master = new_master.replace(r'^([\s])$|^(?![\s\S])$', np.nan, regex=True)
new_master.shape

Total_Records_from_source = len(new_master['email_address'].unique())

#Switch-Case Scenarios
def first_name():
    column_specific = email_specific[['date_created','first_name']]                        [(email_specific['first_name'].isna()==False)]    
    return column_specific
 
def last_name():
    column_specific = email_specific[['date_created','last_name']]                        [(email_specific['last_name'].isna()==False)]
    return column_specific

def address_line1():
    column_specific = email_specific[['date_created','address_line1','address_line2','city','state','country','zip']]                    [(email_specific['address_line1'].isna()==False)]
    return column_specific
 
def phone():
    column_specific = email_specific[['date_created','phone']]                    [(email_specific['phone'].isna()==False)]
    return column_specific

def fax():
    column_specific = email_specific[['date_created','fax']]                    [(email_specific['fax'].isna()==False)]
    return column_specific

def website():
    column_specific = email_specific[['date_created','website']]                    [(email_specific['website'].isna()==False)]
    return column_specific

def is_partner():
    column_specific = email_specific[['date_created','is_partner']]                    [(email_specific['is_partner'].isna()==False)]
    return column_specific

def lead_owner():
    column_specific = email_specific[['date_created','lead_owner']]                    [(email_specific['lead_owner'].isna()==False)]
    return column_specific

def lead_source():
    column_specific = email_specific[['date_created','lead_source']]                    [(email_specific['lead_source'].isna()==False)]
    return column_specific

def order_type():
    column_specific = email_specific[['date_created','order_type']]                    [(email_specific['order_type'].isna()==False)]
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
 

def sub_df(argument):
    # Get the function from switcher dictionary
    func = switcher.get(argument, "nothing")
    # Execute the function
    return func()
 

failed_emails = {}
count_success = 0
count_failed_emails = 0

for i in new_master['email_address'].unique():
#     display(new_master)
    print(i)
    try:
        data = {}

        # Creating a new dataframe using rows that have email_address = i
        email_specific = new_master[new_master['email_address']==i]
    #     print(email_specific.shape,'\n')

        if i == None:
            continue
        
        elif email_specific.shape[0] == 1:
            data = email_specific.to_dict('record')[0]
    #         print(data)

        else:

            for num in range(0,10):
                column_specific = sub_df(num)
    #             display(column_specific)

                # Extract first names to filter out based on frequency for golden record
                col = column_specific.iloc[:,1].tolist()
    #             print('col: ',col,'\n')

                # Get frequency counts of each of the names
                col_count = {x:col.count(x) for x in col}
    #             print("col_count:",col_count,'\n')


                # length of dictionary is greater than length of distinct counts in dictionary, indicates of multiple 
                # distinct values for that attribute >> can easily calculate most frequent value
                # max  value counts == 1 selects only records that do not have ties in duplicate records
                if column_specific.shape[0] == 0:
                    for i in column_specific.columns[1:]:
                        data[i]= np.nan

                elif (len(col_count) > len(set(col_count.values())))                     & (sum(x==max(col_count.values()) for x in col_count.values()) == 1):

                    # Getting most frequent value of column
                    freq_value = max(col_count, key=col_count.get)
    #                 print('freq_value:',freq_value,'\n')

                    # Extracting rows that contain the most frequent value
                    freq_df = column_specific[column_specific.iloc[:,1] == freq_value]
    #                 print('freq_df')
    #                 display(freq_df)

                    val = freq_df.to_dict('records')
                    data.update(val[0])

                else:
                    # Count number of columns that have max non-null values 
                    count = max(column_specific.count(axis=1))
        #             print("count:",count,'\n')

                    # Get rows with maximum number of non-null values
                    min_df = column_specific.dropna(thresh=max(column_specific.count(axis=1)))
                    min_df['date_created'] = pd.to_datetime(min_df['date_created'])#,errors = 'coerce')
    #                 print('min_df')
    #                 print(min_df,'\n')

                    # No date created, take the last value in list
                    if (min_df['date_created'].isnull().values.any() == True):

                        last_value = (min_df.iloc[:,1:].iloc[-1] if len(min_df.iloc[:,1:]) > 1 else min_df.iloc[:,1:])
    #                     print('last_value:','\n',last_value,'\n')
                        val = last_value.to_dict('records')
                        data.update(val[0])

                    else:
                        date_created = max(min_df['date_created'].tolist())
    #                     print('date_created:',date_created)

                        recent_record = min_df[min_df['date_created']==date_created].iloc[:,1:]
                        val = recent_record.to_dict('records')
    #                     display(recent_record,'\n')
                        data.update(val[0])

        # Final dataframe to import into postgres
    #     print('data:','\n')
        data_df = pd.DataFrame(data.items())
        data_df = data_df.set_index(0).T
        data_df = data_df.where(pd.notnull(data_df), None)
    #     display(data_df)

        data_df.to_sql(name='master_customer_test', con=postgres_engine, if_exists='append', index=False, method='multi')
        
        count_success += 1
    
    except UnicodeEncodeError as u:
        i = i.replace(u'\xa0', u' ')
#         count_failed_emails += 1
        
        data = {}

        # Creating a new dataframe using rows that have email_address = i
        email_specific = new_master[new_master['email_address']==i]
    #     print(email_specific.shape,'\n')

        if email_specific.shape[0] == 1:
            data = email_specific.to_dict('record')[0]
    #         print(data)

        else:

            for num in range(0,10):
                column_specific = sub_df(num)
    #             display(column_specific)

                # Extract first names to filter out based on frequency for golden record
                col = column_specific.iloc[:,1].tolist()
    #             print('col: ',col,'\n')

                # Get frequency counts of each of the names
                col_count = {x:col.count(x) for x in col}
    #             print("col_count:",col_count,'\n')


                # length of dictionary is greater than length of distinct counts in dictionary, indicates of multiple 
                # distinct values for that attribute >> can easily calculate most frequent value
                # max  value counts == 1 selects only records that do not have ties in duplicate records
                if column_specific.shape[0] == 0:
                    for i in column_specific.columns[1:]:
                        data[i]= np.nan

                elif (len(col_count) > len(set(col_count.values())))                     & (sum(x==max(col_count.values()) for x in col_count.values()) == 1):

                    # Getting most frequent value of column
                    freq_value = max(col_count, key=col_count.get)
    #                 print('freq_value:',freq_value,'\n')

                    # Extracting rows that contain the most frequent value
                    freq_df = column_specific[column_specific.iloc[:,1] == freq_value]
    #                 print('freq_df')
    #                 display(freq_df)

                    val = freq_df.to_dict('records')
                    data.update(val[0])

                else:
                    # Count number of columns that have max non-null values 
                    count = max(column_specific.count(axis=1))
        #             print("count:",count,'\n')

                    # Get rows with maximum number of non-null values
                    min_df = column_specific.dropna(thresh=max(column_specific.count(axis=1)))
                    min_df['date_created'] = pd.to_datetime(min_df['date_created'])#,errors = 'coerce')
    #                 print('min_df')
    #                 print(min_df,'\n')

                    # No date created, take the last value in list
                    if (min_df['date_created'].isnull().values.any() == True):

                        last_value = (min_df.iloc[:,1:].iloc[-1] if len(min_df.iloc[:,1:]) > 1 else min_df.iloc[:,1:])
                        val = last_value.to_dict('records')
                        data.update(val[0])

                    else:
                        date_created = max(min_df['date_created'].tolist())
    #                     print('date_created:',date_created)

                        recent_record = min_df[min_df['date_created']==date_created].iloc[:,1:]
                        val = recent_record.to_dict('records')
    #                     display(recent_record,'\n')
                        data.update(val[0])

        # Final dataframe to import into postgres
        data_df = pd.DataFrame(data.items())
        data_df = data_df.set_index(0).T
        data_df = data_df.where(pd.notnull(data_df), None)

        data_df.to_sql(name='master_customer_test', con=postgres_engine, if_exists='append', index=False, method='multi')
        
    except Exception as e:
        print(e)
        failed_emails[i]=e
        count_failed_emails += 1
    
    print('-------------------')

insrt_cmd1 = "INSERT INTO LOG_DIM (Phase, Source_Table_Name, Target_Table_Name, Total_Records_Source_Table, Total_Records_Target_Table, Status, Remarks, Execution_time) Values ('StoDW', '3 tables [stg-blc-crm-bpm-customer]', 'dw_master_customer_dim'," +str(Total_Records_from_source)+", "+str(count_success)+",'Completed','"+str(count_failed_emails)+" Failed','"+str(round(time.time() - start_time,2))+" seconds')"
pgcursor.execute(insrt_cmd1)

print("count_failed_emails: ",count_failed_emails)
print("--- %s seconds ---" % (time.time() - start_time))




