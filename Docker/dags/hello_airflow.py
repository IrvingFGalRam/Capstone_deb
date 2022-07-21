from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

def p_hello_airflow():
    return 'Hello world from your first ORIGIN Airflow DAG!'

dag = DAG('hello_airflow', description='Hello Airflow World DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2017, 3, 20), catchup=False)

hello_airflow_operator = PythonOperator(task_id='hello_airflow_task', python_callable=p_hello_airflow, dag=dag)

hello_airflow_operator