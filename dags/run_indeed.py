import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta

args = {
    'owner': 'wilmer',
    'depends_on_past': False,
    'start_date': datetime(2019,8,12),
    'email': ['yangw8989@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=15),
    'schedule_interval': '@daily',
}

dag = DAG(
    dag_id='indeed_etl',
    default_args=args,
    schedule_interval='@daily',
    dagrun_timeout=timedelta(minutes=60),
)


t0 = BashOperator(
    task_id='indeed_etl',
    bash_command='python3 /root/airflow/dags/indeed_v1_5.py',
    dag=dag)

