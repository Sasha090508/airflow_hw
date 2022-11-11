import datetime as dt
import os
import sys

from airflow.models import DAG
from airflow.operators.python import PythonOperaror
from airflow.operators.bash import BashOperator

path = '/airflow_hw'
# Добавим путь к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавим путь к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)

from modules.pipeline import pipeline
from modules.predict import predict

args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
    'depends_on_past': False,
}

with DAG(
        dag_id='car_price_prediction',
        schedule_interval="00 15 * * *",
        default_args=args,
) as dag:

    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "PYSK!"',
        dag=dag,
    )
    pipeline = PythonOperator(
        task_id='pipeline',
        python_callable=pipeline,
        dag=dag,
    )
    prediction = PythonOperaror(
        task_id='predict',
        python_callable=predict,
        dag=dag,
    )
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "STOP"',
    )

    first_task >> pipeline >> prediction >> last_task

