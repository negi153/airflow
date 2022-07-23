from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

# Instantiate your DAG
dag = DAG('xcom_example')

def retrieve_watermark(**kwargs):
    kwargs['ti'].xcom_push(key='ga360_historical_start_date',value=kwargs['start_date'])
    kwargs['ti'].xcom_push(key='ga360_historical_end_date',value=kwargs['end_date'])

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


    retrieveWatermark = PythonOperator(
        task_id='retrieve_watermark',
        # provide context for XCom
        provide_context=True,
        python_callable=retrieve_watermark,
        op_kwargs = {
            'start_date' : "{{ dag_run.conf and dag_run.conf.process_date}}",
            'end_date' : "{{ dag_run.conf and dag_run.conf.process_end_date}}"
        }
    )
    # Run query using the XCom value
    runSQL = BigQueryOperator(
         task_id='insert-into-table',
         destination_dataset_table='project.dataset.table',
         sql="SELECT * FROM source WHERE created_at > '{{ task_instance.xcom_pull(task_ids='retrieveWatermark', key='watermark') }}'”
     )
     retrieveWatermark >> runSQL