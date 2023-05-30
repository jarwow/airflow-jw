###
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}
# Define Func logic


def extract(**kwargs):
    ti = kwargs['ti']
    data_string = '{"1001": 100, "1002": 200, "1003": 300}'
    ti.xcom_push('order_data', data_string)


def transform(**kwargs):
    import json
    from time import sleep

    ti = kwargs['ti']
    extract_data_string = ti.xcom_pull(task_ids='extract', key='order_data')
    order_data = json.loads(extract_data_string)

    total_order_value = 0
    for value in order_data.values():
        total_order_value += value
        print(f'sleep 20sec.... total_order_value : {total_order_value}')
        sleep(20)

    total_value = {"total_order_value": total_order_value}
    total_value_json_string = json.dumps(total_value)
    ti.xcom_push('total_order_value', total_value_json_string)


def load(**kwargs):
    import json

    ti = kwargs['ti']
    total_value_string = ti.xcom_pull(task_ids='transform', key='total_order_value')
    total_order_value = json.loads(total_value_string)

    print(total_order_value)


# ================================== START DAG ======================================
with DAG(
    dag_id='demo_traditional_dag_etl',
    default_args=default_args,
    description='ETL DAG tutorial',
    schedule_interval=None,
    start_date=days_ago(2),
    catchup=False,
    tags=['aiteam', 'demo'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    # Define workflow
    extract_task >> transform_task >> load_task
