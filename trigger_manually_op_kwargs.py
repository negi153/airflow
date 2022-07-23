from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import json


def show(**kwargs):
    print('kwargs',kwargs)
    print(type(kwargs))

    ti = kwargs['ti']
    
    data ={
        'dag_start_date':kwargs['start_date'],
        'dag_end_date':kwargs['end_date']
    }

    ti.xcom_push(key='ga360_historical_parms',value=json.dumps(data))
    # print('not pushing xcom')


def show2(**kwargs):
    ti = kwargs['ti']

    data = ti.xcom_pull(key='ga360_historical_parms', task_ids='push')
    print(data)
    print(type(data))
    
    print('after conversion')
    
    data2=json.loads(data)
    print(data2)
    print(type(data2))


args = {
    'owner': 'Airflow',
    'start_date': datetime(2022,1,1),
    'provide_context': True
}


with DAG(
    dag_id='testing_trigger_dag',
    default_args=args,
    schedule_interval=None,
    tags=['example']
) as dag :

    start = DummyOperator(task_id='start_dag')

    t1=PythonOperator(
        task_id='push',
        python_callable=show,
        op_kwargs = {
            'start_date' : "{{ dag_run.conf and dag_run.conf.process_date}}",
            'end_date' : "{{ dag_run.conf and dag_run.conf.process_end_date}}"
        }
    )

    t2=PythonOperator(
        task_id='showval',
        python_callable=show2
    )

    end = DummyOperator(task_id='end_dag')
    
    start >> t1 >> t2 >>  end


