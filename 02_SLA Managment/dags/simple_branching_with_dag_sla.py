from datetime import datetime, timedelta
from time import sleep
from random import choice

import logging

from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator


default_args = {
    'owner': 'loonycorn',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['cloud.user@loonycorn.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(seconds=5)
}

logger = logging.getLogger(__name__)

def sla_missed_action(*args, **kwargs):
  logger.info('*********************************************************')
  logger.info('=========================WARNING=========================')
  logger.info('===============   SLA MISSED DAG LEVEL   ===============')
  logger.info('*********************************************************')


def choose_branch():

    return choice([True, False])

def branch(ti):
    choice = ti.xcom_pull(task_ids='taskChoose')

    sleep(3)

    if choice:
        return 'taskC'
    else:
        return 'taskE'

def task_c():
    
    print("TASK C executed!")



with DAG(
    dag_id = 'simple_branching_with_dag_sla',
    description = 'Simple branching pipeline with DAG SLAs',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '*/1 * * * *',
    tags = ['pipeline', 'branching', 'sla', 'dag'],
    sla_miss_callback = sla_missed_action,
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'echo TASK A has executed!'
    )

    taskChoose = PythonOperator(
        task_id = 'taskChoose',
        python_callable = choose_branch
    )

    taskBranch = BranchPythonOperator(
        task_id = 'taskBranch',
        python_callable = branch
    )

    taskC = PythonOperator(
        task_id = 'taskC',
        python_callable = task_c
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'sleep 20',
    )

    taskE = EmptyOperator(
        task_id = 'taskE'
    )


taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD