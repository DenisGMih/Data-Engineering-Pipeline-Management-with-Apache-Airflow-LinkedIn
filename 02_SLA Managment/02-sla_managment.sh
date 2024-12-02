-------------------------------------------------------------------------------
# Version 01
-------------------------------------------------------------------------------

# Before we get started with SLA lets setup our email to send emails throught airflow

# In an incognito window login to loony.test.001@gmail.com (you may need to call me for OTP)

# Show that you are on the gmail account page

# Open a new tab and go to this URL: 

https://myaccount.google.com/u/2/security

# Here show that 2-step verification is turned on


# Now go to this page

https://myaccount.google.com/apppasswords


# At the bottom, click Select app and choose the app you’re using (MAIL, MAC)

# Select Generate.

# Copy over the 16 digit password

# Click Done


-------------------------------------------------------------
# Open up airflow.cfg in Sublimetext and add these settings

# Search for the [smtp] section


smtp_host = smtp.gmail.com
smtp_starttls = True
smtp_ssl = False
smtp_user = loony.test.001@gmail.com
smtp_password = 16_DIGIT_APP_PASSWORD
smtp_port = 587
smtp_mail_from = loony.test.001@gmail.com
smtp_timeout = 30
smtp_retry_limit = 5


# Search for check_slas  and make sure that it is true

check_slas = True

# IMPORTANT: Stop and restart the "scheduler" and "webserver" in the terminal


-------------------------------------------------------------
# Notes
# SLA for tasks is counter intuitive
# You would expect that, by setting a SLA for a task, you’re defining the expected duration for that task. However, instead of using the start time of the task, Airflow uses the start time of the DAG.


# Now let's use our simple_branching_with_task_sla.py DAG


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
    'retry_delay': timedelta(minutes=5)
}

logger = logging.getLogger(__name__)

def sla_missed_action(*args, **kwargs):
  logger.info('*********************************************************')
  logger.info('=========================WARNING=========================')
  logger.info('===============   SLA MISSED TASK LEVEL   ===============')
  logger.info('*********************************************************')


def choose_branch():

    return choice([True, False])

def branch(ti):
    choice = ti.xcom_pull(task_ids='taskChoose')

    # if choice:
    #     return 'taskC'
    # else:
    #     return 'taskE'

    return 'taskC'

def task_c():
    
    sleep(20)

    print("TASK C executed!")



with DAG(
    dag_id = 'simple_branching_with_sla',
    description = 'Simple branching pipeline with SLAs',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '*/1 * * * *',
    tags = ['pipeline', 'branching', 'sla'],
    sla_miss_callback = sla_missed_action
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
        python_callable = task_c,
        sla = timedelta(seconds=5)
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'echo TASK D has executed!',
    )

    taskE = EmptyOperator(
        task_id = 'taskE'
    )


taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD



# Go to the Airflow UI (make sure you are logged in as the Admin user)

# In the Grid view

# Unpause the simple_branching_with_task_sla DAG

# Wait for it to run through 3-4 times (it will run every minute)

# Click on TaskC for one of the runs and show that it takes 20 seconds to run

# From the navigation bar

Goto "Browse" -> "SLA Misses"

# Observe there are multiple found for SLA Misses (all of them triggered because of task C)

# Log in to cloud.user@loonycorn.com

# Show the email that was received for SLA misses


# Open the log file in Sublimetext

# > Now goto "$AIRFLOW_HOME/logs/scheduler/[--DATE--]/simple_branching_with_task_sla.py.log"

Search for "==WARNING=="

# Observe this was successfully logged

# Pause the simple_branching_with_task_sla


----------------------------------------------------------------------------------------

# Notes

# SLA only works for scheduled DAGs
# If your DAG has no schedule, SLAs will not work. Also, even if your DAG has a schedule but you trigger it manually, Airflow will ignore your run’s start_date and pretend as if your run was scheduled. This happens because Airflow only considers the schedule execution time of each DAG run and the interval between runs. If your DAG does not have an interval between runs, meaning there is no schedule, Airflow fails to calculate the execution dates, leading it to miss any triggered runs.

# We schedule our DAG to run every minute


# Now let's use our simple_branching_with_dag_sla.py DAG


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



# Go to the Airflow UI (make sure you are logged in as the Admin user)

# In the Grid view

# Unpause the simple_branching_with_dag_sla DAG

# Wait for it to run through 3-4 times (it will run every minute)

# From the navigation bar

Goto "Browse" -> "SLA Misses"

# Observe there are multiple found for SLA Misses (they're triggered because of different tasks)

# Log in to cloud.user@loonycorn.com

# Show the email that was received for SLA misses


# Open the log file in Sublimetext

# > Now goto "$AIRFLOW_HOME/logs/scheduler/[--DATE--]/simple_branching_with_dag_sla.py.log"

Search for "==WARNING=="

# Observe this was successfully logged

# Pause the simple_branching_with_dag_sla

----------------------------------------------------------------------------------------

# Have the DAG level SLA as before

# Wire up an on_failed_callback which uses the email operator to send an email

# Randomly fail the branch task by throwing an exception

# Show file simple_branching_with_sla_email.py


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
        bash_command = 'sleep 20',
    )

    taskE = EmptyOperator(
        task_id = 'taskE'
    )


taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD


# Go to the Airflow UI 

# In the Grid view

# Unpause the simple_branching_with_sla_email DAG

# Wait for it to run through 344 times till there is at least 2-3 failures (it will run every minute)

# From the navigation bar

Goto "Browse" -> "SLA Misses"

# Observe there are multiple found for SLA Misses (they're triggered because of different tasks)

# Log in to cloud.user@loonycorn.com

# Show the email that was received for SLA misses

# Also show that we receive an email for task failure as well

