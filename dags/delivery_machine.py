from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from .handlers import command
from .handlers import machine

default_args = {
    'owner': 'zhengshuai',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
}

dag = DAG(
    'delivery_machine',
    default_args=default_args,
    description='交付机器',
    schedule_interval=None,
)


"""
POST /api/v1/scheduler/<dag_name>
content-type: application/json
Authorization: JWT xxx
{

}
"""

tx = PythonOperator(
    task_id='check_instance',
    provide_context=True,
    python_callable=machine.check_instance_handler,
    dag=dag,
)

t0 = PythonOperator(
    task_id='create_instance',
    provide_context=True,
    python_callable=machine.create_instance_handler,
    dag=dag,
)

t1 = PythonOperator(
    task_id='system_init',
    provide_context=True,
    python_callable=command.system_init_handler,
    dag=dag,
)

t2 = PythonOperator(
    task_id='wait_system_init_status',
    provide_context=True,
    python_callable=command.wait_system_init_status_handler,
    retries=5,
    retry_delay=timedelta(seconds=30),
    dag=dag,
)

t3 = PythonOperator(
    task_id='application_init',
    provide_context=True,
    python_callable=command.application_init_handler,
    dag=dag,
)

t4 = PythonOperator(
    task_id='wait_application_init_finish',
    provide_context=True,
    python_callable=command.wait_application_init_finish_handler,
    retries=5,
    retry_delay=timedelta(seconds=60),
    dag=dag,
)

tx >> t0 >> t1 >> t2 >> t3 >> t4
