from datetime import datetime, timedelta
from time import sleep
from random import choice

import logging

from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.operators.email import EmailOperator
from airflow.utils.email import send_email



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

def sla_missed_action(context):
    logger.info('*******************************************************')
    logger.info('========================WARNING========================')
    logger.info('===============   SLA MISSED DAG LEVEL   ===============')
    logger.info(f"""
        DAG: {context['dag'].dag_id}
        Task: {context['task_instance'].task_id}
        Execution Date: {context['task_instance'].execution_date}
        Expected Duration: {context['task_instance'].task.sla}
        Actual Duration: {context['task_instance'].duration}
    """)
    logger.info('*******************************************************')


def task_failed_action(context):
    logger.info(f"""
        DAG: {context['dag'].dag_id}
        Task: {context['task_instance'].task_id}
        Execution Date: {context['task_instance'].execution_date}
    """)

    subject = f"Failed for Task: {context['task_instance'].task_id}"

    body = f"""
        DAG: {context['dag'].dag_id}
        Task: {context['task_instance'].task_id}
        Execution Date: {context['task_instance'].execution_date}
    """

    email_operator = EmailOperator(
        task_id='send_email',
        to=['cloud.user@loonycorn.com'],
        subject=subject,
        html_content=body,
        mime_subtype='html'
    )
    email_operator.execute(context)


def choose_branch():

    return choice([True, False])

def branch(ti):
    if choice([0, 1]) == 1:
        raise Exception("Intentional task failure")

    choice = ti.xcom_pull(task_ids='taskChoose')

    if choice:
        return 'taskC'
    else:
        return 'taskE'

def task_c():
    
    print("TASK C executed!")



with DAG(
    dag_id = 'simple_branching_with_sla_email',
    description = 'Simple branching pipeline with DAG SLAs and email on failure',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '*/1 * * * *',
    tags = ['pipeline', 'branching', 'sla', 'dag', 'email'],
    sla_miss_callback = sla_missed_action,
    on_failure_callback = task_failed_action,
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
        bash_command = 'sleep 8',
    )

    taskE = EmptyOperator(
        task_id = 'taskE'
    )


taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD