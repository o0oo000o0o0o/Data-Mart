#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg2
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, inspect
import pandas as pd
import time


# In[2]:


postgres_engine = create_engine('postgresql://praveen:Admin123@165.22.220.96:5432/staging')


# In[3]:


crm = pd.read_sql('SELECT * FROM stg_crm_customer', con=postgres_engine)
bpm = pd.read_sql('SELECT * FROM stg_bpm_client_info', con=postgres_engine)
blc = pd.read_sql('SELECT * FROM stg_customer', con=postgres_engine)
master = pd.read_sql('SELECT * FROM dw_master_customer_dim', con=postgres_engine)


# In[4]:


master.columns


# In[5]:


crm["source_table"] = "CRM_Customer"
crm["role_id"] = 1
crm = crm.rename(columns={"locality": "city", "region": "state", "country_code": "country", "given_name": "first_name", "phonenumber": "phone", "address1": "address_line1", "address2": "address_line2", "zip_code": "zip"})
crm = crm[['customer_id', 'email_address', 'date_created', 'first_name',
       'address_line1', 'address_line2', 'city', 'state', 'zip',
       'country', 'phone', 'email_status', 'created_date', 'updated_date',
       'source_table', 'role_id']]
crm


# In[6]:


new_master = master.append(crm)
new_master


# In[7]:


bpm.columns


# In[8]:


bpm["source_table"] = "BPM_Client_INFO"
bpm["role_id"] = 1
bpm = bpm.rename(columns={"company_name": "publisher_name", "email": "email_address", "address": "address_line1", "date": "date_of_sale", "clientid": "customer_id"})
bpm = bpm[['customer_id', 'publisher_name', 'address_line1', 'city', 'state',
       'country', 'zip', 'phone', 'fax', 'email_address',
       'date_of_sale', 'created_date', 'updated_date', 'source_table',
       'role_id']]
bpm


# In[9]:


new_master = new_master.append(bpm)
new_master


# In[10]:


blc.columns


# In[11]:


blc["source_table"] = "BLC_Customer_Updated"


# In[12]:


blc = blc[['customer_id', 'date_created','deactivated', 'email_address',
       'first_name', 'last_name', 'password', 'user_name', 'address_line1', 'city', 'phone', 'zip', 'country_name', 'state_name', 'role_id',
       'is_publisher', 'publisher_name', 'website', 'is_partner', 'date_of_sale',
       'lead_owner', 'lead_source', 'order_type', 'created_date',
       'updated_date', 'source_table']]
blc = blc.rename(columns={"country_name": "country", "state_name": "state"})


# In[13]:


blc = blc.loc[(blc["role_id"] == 1) & pd.notnull(blc["address_line1"])]


# In[14]:


new_master = new_master.append(blc)
new_master


# In[15]:


new_master["master_customer_id"] = range(1, 1+len(new_master))
new_master


# In[16]:


new_master.to_sql(name='dw_master_customer_dim', con=postgres_engine, if_exists='append', index=False, method='multi')



