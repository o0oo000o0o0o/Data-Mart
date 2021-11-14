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


dag = DAG('HistoricalLtoSTest',default_args = default_args,schedule_interval = '@once')

task_1 = BashOperator(task_id = 'start_task',
     bash_command = 'echo start_task',
     dag = dag)

task_2 = BashOperator(task_id = 'BLC_STORES', 
dag = dag,
bash_command = 'python3 /root/Mahathi/LtoSHistorical/SA_BLC_STORES.py')

task_3 = BashOperator(task_id = 'BLC_STATES', 
dag = dag,
bash_command = 'python3 /root/Mahathi/LtoSHistorical/SA_BLC_STATES.py')

task_4 = BashOperator(task_id = 'BLC_COUNTRIES', 
dag = dag,
bash_command = 'python3 /root/Mahathi/LtoSHistorical/SA_BLC_COUNTRIES.py')

task_5 = BashOperator(task_id = 'BLC_ORDER', 
dag = dag,
bash_command = 'python3 /root/Mahathi/LtoSHistorical/SA_BLC_ORDERS.py')

task_6 = BashOperator(task_id = 'CONVERSION_ORDER_MASTER', 
dag = dag,
bash_command = 'python3 /root/Mahathi/LtoSHistorical/SA_BLC_CONVERSION_ORDER_MASTER.py')

task_7 = BashOperator(task_id = 'DISTRIBUTION_ORDERINFO', 
dag = dag,
bash_command = 'python3 /root/Mahathi/LtoSHistorical/SA_BLC_DISTRIBUTION_ORDERINFO.py')

task_8 = BashOperator(task_id = 'BPM_CLIENTINFO', 
dag = dag,
bash_command = 'python3 /root/Mahathi/LtoSHistorical/SA_BPM_CLIENTINFO.py')

task_9 = BashOperator(task_id = 'CRM_CUSTOMER', 
dag = dag,
bash_command = 'python3 /root/Mahathi/LtoSHistorical/SA_CRM_CUSTOMER.py')

task_10 = BashOperator(task_id = 'BLC_CUSTOMER', 
dag = dag,
bash_command = 'python3 /root/Mahathi/LtoSHistorical/SA_BLC_CUSTOMER.py')

task_11 = BashOperator(task_id = 'BLC_SALES', 
dag = dag,
bash_command = 'python3 /root/Mahathi/LtoSHistorical/SA_BLC_SALESREPORTS.py')

task_12 = DummyOperator(task_id='End_task',
        bash_command = 'echo Upload Completed',
     dag = dag)

#Task Order
task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6 >> task_7 >> task_8 >> task_9 >> task_10 >> task_11 >> task_12

