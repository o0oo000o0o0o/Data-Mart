#!/usr/bin/env python
# coding: utf-8

from airflow import DAG
from  airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from time import time, sleep
from datetime import timedelta


default_args = {
    'owner': 'transdata',
    'start_date': datetime(2020, 6, 10),
    'retry_delay': timedelta(minutes=5)
}


dag = DAG('HistoricalStoDWTest',default_args = default_args,schedule_interval = '@once')

task_1 = BashOperator(task_id = 'start_task',
     bash_command = 'echo start_task',
     dag = dag)

task_2 = BashOperator(task_id = 'STORES_DIM', 
dag = dag,
bash_command = 'python3 /root/Mahathi/StoDWHistorical/DW_STORES.py')

task_3 = BashOperator(task_id = 'DISTRIBUTION_ORDERINFO_DIM', 
dag = dag,
bash_command = 'python3 /root/Mahathi/StoDWHistorical/DW_DISTRIBUTION_ORDERINFO.py')

task_4 = BashOperator(task_id = 'MASTER_CUSTOMER_DIM', 
dag = dag,
bash_command = 'python3 /root/Mahathi/StoDWHistorical/MASTER_CUSTOMER_DIM.py')

task_5 = BashOperator(task_id = 'COMPANY_NAME_DIM', 
dag = dag,
bash_command = 'python3 /root/Mahathi/StoDWHistorical/DW_REF_COMPANY_NAME_DIM.py')

task_6 = BashOperator(task_id = 'SOURCE_DIM', 
dag = dag,
bash_command = 'python3 /root/Mahathi/StoDWHistorical/DW_REF_SOURCE_DIM.py')

task_7 = BashOperator(task_id = 'SALES_FACT', 
dag = dag,
bash_command = 'python3 /root/Mahathi/StoDWHistorical/DW_SALES_FACT.py')

task_8 = DummyOperator(task_id='End_task',
        bash_command = 'echo Upload Completed',
     dag = dag)

#Task Order
task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6 >> task_7 >> task_8


