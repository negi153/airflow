from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime

def show():
    print('hello')

args = {
    'owner': 'Airflow',
    'start_date': datetime(2022,1,1),
}

with DAG(
    dag_id='testing_first_dag',
    default_args=args,
    schedule_interval=None,
    tags=['example']
) as dag :

    start = DummyOperator(task_id='start_dag')

    t1=PythonOperator(
         task_id='push',
        python_callable=show,
    )

    end = DummyOperator(task_id='end_dag')
    
    start >> t1 >> end


