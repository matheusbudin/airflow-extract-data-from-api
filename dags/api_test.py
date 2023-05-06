#imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import requests
import pandas as pd
import json

#definitions
MY_NAME = "Matheus"
number_of_rows = 1000

#python function, api data retrieve
#it is good practice to separate functions by functionality, each function should do only one thing

def data_from_api(number_of_rows: int):
  #make a request to faker API to return user data:
  response = requests.get(f'https://fakerapi.it/api/v1/users?_quantity={number_of_rows}')
  #debugging:
  print(response)
  
  #convert the extraction file to json format:
  data = response.json()
  
  transformed_data = []

  #loop throught each record from json file and extract desired fields:
  for record in data['data']:
    transformed_record = {
        'Name': record['firstname'] + ' ' + record['lastname'],
        'Email': record['email'],
        'IP': record['ip']
    }
    transformed_data.append(transformed_record)
  
  return transformed_data[0:4]

def transform_json(data: json):
  #creates empty list to store transformated data
  transformed_data = []

  #loop throught each record from json file and extract desired fields:
  for record in data['data']:
    transformed_record = {
        'Name': record['firstname'] + ' ' + record['lastname'],
        'Email': record['email'],
        'IP': record['ip']
    }
    transformed_data.append(transformed_record)
  return transformed_data






with DAG(
    dag_id = 'extract_api_data',
    start_date=datetime(2023,5,6),
    schedule=timedelta(minutes=30),
    catchup=False,
    tags=["personal-project"],
    default_args={
        "owner": MY_NAME,
        "retries": 2,
        "retry_delay": timedelta(minutes=5)
    }
) as dag:

  t1 = PythonOperator(
          task_id = "returns-data-from-an-API",
          python_callable=data_from_api,
          op_kwargs = {"number_of_rows": number_of_rows},
      ) 
  
#set dependencies
#t1>>t2