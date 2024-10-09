import datetime as dt
import pandas as pd
import psycopg2 as db

from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from elasticsearch import Elasticsearch

def queryPostgresql():
    conn_string = "dbname='airflow' user='airflow' password='airflow' host='postgres' port='5432'"
    conn = db.connect(conn_string)
    df = pd.read_sql("SELECT name, city FROM users", conn)
    df.to_csv('postgresqldata.csv', index=False)
    print("-------Data Saved------")

def insertElasticsearch():
    es = Elasticsearch(
        ["http://elasticsearch:9200"],
        request_timeout=30,  # Use request_timeout instead of timeout
        max_retries=10,
        retry_on_timeout=True
        )  # Ensure Elasticsearch hostname matches Docker setup
    df = pd.read_csv('postgresqldata.csv')
    for _, row in df.iterrows():
        doc = row.to_dict()  # Convert each row to a dictionary for Elasticsearch
        res = es.index(index="frompostgresql", body=doc)
        print(res)

default_args = {
    'owner': 'ahmadarbain',
    'start_date': dt.datetime(2024, 9, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('MyDBdag',
         default_args=default_args,
         schedule_interval=timedelta(minutes=5)) as dag:
    
    getData = PythonOperator(
        task_id='QueryPostgreSQL',
        python_callable=queryPostgresql
    )

    insertData = PythonOperator(
        task_id='InsertDataElasticsearch',
        python_callable=insertElasticsearch
    )

    getData >> insertData
