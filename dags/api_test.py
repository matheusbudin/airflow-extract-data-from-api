#imports
import os
import requests
import pandas as pd
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator


#definitions
MY_NAME = "Matheus"
number_of_rows = 1000
postgres_conn_id = "postgres"

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

  #df_api = pd.DataFrame(transformed_data)
  # #saving it as a local file:
  #storing as a csv file (to load inside a db for example or AWS S3)
  #df_api.to_csv('./my_api_data.csv', index=False, mode='w+')
  #sorting as a .parquet (to save space -> optimal in cloud storage, save resources)
  #df_api.to_parquet('./my_api_data.parquet', index=False)

  return transformed_data

#next step is save those data frames into postgres

def store_in_postgres():
  #convert the data to a dataframe:
  data = data_from_api(number_of_rows)
  df_api = pd.DataFrame(data)

  #connect to Postgres and create a table
  hook = PostgresHook(postgres_conn_id = postgres_conn_id)
  conn = hook.get_conn()
  cursor = conn.cursor()
  cursor.execute(sql="CREATE TABLE IF NOT EXISTS my_table (Name varchar, Email varchar, IP varchar)")
  conn.commit()

  #insert the dataframe into the table
  #df_api.to_sql('my_table', hook.get_sqlalchemy_engine(), if_exists='append', index=False)
  #df_api.to_sql('my_table', con=postgres_conn_id, if_exists='replace', index=False)

#create a postgresOperator task to execute a SQL statement to create a table




with DAG(
    dag_id = 'extract_api_data',
    start_date=datetime(2023,5,6),
    schedule=None, #timedelta(minutes=30),
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
  # t2 = PythonOperator(
  #   task_id='store_data_in_postgres',
  #   python_callable=store_in_postgres,
  # )
  
  create_table_task = PostgresOperator(
    task_id='create-table',
    postgres_conn_id='postgres',
    sql='''
        CREATE TABLE IF NOT EXISTS my_table (
        
            Name varchar(60),
            Email varchar(30),
            IP varchar(20)
        )
    
    '''
  )
#set dependencies
t1>>create_table_task


#to do:
# need to figure out the postgres conection
# error: port 5432 failed: Cannot assign requested address Is the server running on that host and accepting TCP/IP connections?