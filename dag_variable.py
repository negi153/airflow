from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import json
from airflow.models import Variable

date_range = Variable.get('ga360_historical_date_range')

def show():
    print(date_range)
    print(type(date_range))



args = {
    'owner': 'Airflow',
    'start_date': datetime(2022,1,1)
}


with DAG(
    dag_id='testing_variable_dag',
    default_args=args,
    schedule_interval=None,
    tags=['example']
) as dag :

    start = DummyOperator(task_id='start_dag')

    t1=PythonOperator(
        task_id='push',
        python_callable=show
        )

    end = DummyOperator(task_id='end_dag')
    
    start >> t1 >>  end


