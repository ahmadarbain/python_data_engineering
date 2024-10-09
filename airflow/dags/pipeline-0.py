import pandas as pd

import datetime as dt
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def CSVToJson():
    df = pd.read_csv(r"/opt/airflow/dataset/data.csv")
    for i, r, in df.iterrows():
        print(r['name'])
    df.to_json('fromAirflow.JSON', orient='records')

default_args = {
    'owner': 'ahmadarbain',
    'start_date': dt.datetime(2024, 9, 7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

with DAG(
    dag_id='MyCSVDAG',
    default_args=default_args,
    schedule_interval=timedelta(minutes=5),
    # '0 * * * *',
) as dag:
    print_starting = BashOperator(
        task_id='starting',
        bash_command='echo "I am reading the CSV now"'
    )

    csvJson = PythonOperator(
        task_id='convertCSVtoJson',
        python_callable=CSVToJson   
    )

print_starting >> csvJson


