import requests
import pandas as pd
import datetime as dt

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

# def get_data(url, output):
#     # Make a GET request to download the file
#     response = requests.get(url)

#     # Check if the request was successful (status code 200)
#     if response.status_code == 200:
#         with open(os.path.join(output, 'scooter.csv'), 'wb') as file:
#             file.write(response.content)
#         print(f"File downloaded successfully and saved to {os.path.join(output, 'scooter.csv')}")
#     else:
#         print(f"Failed to download the file. Status code: {response.status_code}")

def cleanScooter():
    df = pd.read_csv(r'/opt/airflow/dataset/scooter.csv')  # Update path
    df.head(5)
    df.drop(columns=['region_id'], inplace=True)
    df.columns = [x.lower() for x in df.columns]
    df['started_at'] = pd.to_datetime(df['started_at'], format='%m/%d/%Y %H:%M')
    df.to_csv(r'/opt/airflow/dataset/cleanscooter.csv', index=False)  # Update path

def filterData():
    df = pd.read_csv(r'/opt/airflow/dataset/cleanscooter.csv')  # Update path
    fromd = '2019-05-23'
    tod = '2019-06-03'
    tofrom = df[(df['started_at'] > fromd) & (df['started_at'] < tod)]
    tofrom.to_csv(r'/opt/airflow/dataset/may23-june3.csv', index=False)  # Update path

default_args = {
    'owner': 'ahmadarbain',
    'start_date': dt.datetime(2024, 9, 30),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5)
}

with DAG(
    'CleanData',
    default_args=default_args,
    description='Cleansing, Transforming, & Loading data',
    schedule_interval=timedelta(minutes=5),
    tags=['ETL-v1'],
) as dag:
    
    # # Task to download the file
    # downloadData = PythonOperator(
    #     task_id='download',
    #     python_callable=get_data,
    #     retries=3,
    #     retry_delay=timedelta(minutes=1), 
    #     op_kwargs={
    #         'url': 'https://raw.githubusercontent.com/PaulCrickard/escooter/master/scooter.csv',
    #         'output': r'D:\PYTHON DATA ENGINEER\src\dataset'  # Use raw string for Windows path
    #     }
    # )
    
    # Task to clean the data
    cleanData = PythonOperator(
        task_id='clean',
        python_callable=cleanScooter
    )
    
    # Task to filter the data
    selectData = PythonOperator(
        task_id='filter',  # Removed extra 'j' character
        python_callable=filterData
    )
    
    # Task to move the file
    moveFile = BashOperator(
        task_id='move',
        bash_command='mv /opt/airflow/dataset/may23-june3.csv /opt/airflow/'  # Update this path based on your OS
    )

# Task Dependencies
cleanData >> selectData >> moveFile
