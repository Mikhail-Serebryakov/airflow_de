import pandas as pd
from sqlalchemy import create_engine
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

conn = create_engine('postgresql+psycopg2://airflow:airflow@puckelairflow_postgres_1:5432/airflow')


dag = DAG(
    dag_id="view_data",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval=None
)


def view():
    df = pd.read_sql("SELECT * FROM currency_table ORDER BY date DESC LIMIT 5;", con=conn)
    print(df)


view_data = PythonOperator(
    task_id="view",
    python_callable=view,
    dag=dag
)

view_data
