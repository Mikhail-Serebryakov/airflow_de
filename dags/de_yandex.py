import requests
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt

conn = create_engine('postgresql+psycopg2://airflow:airflow@airflow_de_postgres_1:5432/airflow')

dag = DAG(
    dag_id="get_data",
    start_date=dt.datetime(2021, 1, 1),
    schedule_interval='@daily'
)


def ex(ds):
    url = f"https://api.exchangerate.host/{ds}"
    params = {'symbols': 'USD',
               'base': 'BTC',
              'format': 'csv'
              }
    response = requests.get(url, params=params)
    df = pd.read_csv(StringIO(response.text))
    df['currency_pair'] = df['base'].astype(str) + '/' + df['code']
    df['date'] = pd.to_datetime(df['date'])
    df['rate'] = df['rate'].astype(str).apply(lambda x: float(x.replace(',', '.')))
    df = df[['currency_pair', 'date', 'rate']]
    df.to_sql('currency_table',  con=conn, if_exists='append', index=False)


get_data = PythonOperator(
    task_id="get_currency",
    python_callable=ex,
    op_args=['{{ ds }}'],
    retries=3,
    dag=dag
)

get_data
