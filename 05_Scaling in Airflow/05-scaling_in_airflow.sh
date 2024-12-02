# https://towardsdatascience.com/a-gentle-introduction-to-understand-airflow-executor-b4f2fee211b1
# https://medium.com/international-school-of-ai-data-science/executors-in-apache-airflow-148fadee4992
# https://medium.com/international-school-of-ai-data-science/celery-executor-in-apache-airflow-da070b9ced28


# In terminal

$ airflow info | grep "apache-airflow-providers*"


# Observe we have celery already installed when we installed airflow
# in the begining using this command 

# pip install "apache-airflow[celery]==2.5.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.5.3/constraints-3.7.txt"


# Celery Executor: The Celery Executor allows tasks to be executed on a separate
# pool of worker nodes, allowing for parallel execution and improved scalability.
# Tasks are dispatched by the scheduler to the Celery workers, which execute the
# tasks and return the results to the scheduler.
# This executor is designed for large-scale, complex workflows and is the preferred
# executor for such use cases.
# The Celery Executor distributes the workload from the main application onto multiple
# celery workers with the help of a message broker such as RabbitMQ or Redis.
# The executor publishes a request to execute a task in the queue, and one of
# several worker nodes picks up the request and runs as per the directions provided.


-------------------------------------------------------

# In new terminal tab

$ psql

$ CREATE DATABASE cars_db;

$ \c cars_db

$ \dt


# In the Airflow UI

# Click on "+" button

Connection Id -> cars_postgres_conn
Connection Type -> Postgres
Description -> Postgres connection to the cars database
Host -> localhost
Schema -> cars_db
Login -> loonycorn
Password -> password
Port -> 5432

# Click on test to test the connection

# Click on Save


# IMPORTANT: For the code, have all the code set up behind the scenes


# sql_statements/create_table_car.sql
CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    registration VARCHAR(10),
    car_make VARCHAR(50),
    car_model VARCHAR(50),
    car_model_year INT,
    color VARCHAR(50),
    mileage INT,
    price DECIMAL(10, 2),
    transmission VARCHAR(20),
    fuel_type VARCHAR(20),
    condition VARCHAR(50),
    location VARCHAR(50)
);



# sql_statements/insert_car_data.sql
COPY {{ params.table_name }} (
    registration, car_make, car_model, 
    car_model_year, color, mileage, price, 
    transmission, fuel_type, condition, location
)
FROM {{ params.csv_path }}
WITH (FORMAT csv, HEADER true)
WHERE condition = {{ params.condition }};



# data_processing_pipeline.py
import pandas as pd

from random import choice

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable


default_args = {
   'owner': 'loonycorn'
}


ORIGINAL_DATA_PATH = '/Users/jananiravi/airflow/datasets/cars_details.csv'
CLEANED_DATA_PATH = '/Users/jananiravi/airflow/datasets/cleaned_car_details.csv'
OUTPUT_PATH = '/Users/jananiravi/airflow/output/{0}.csv'

def read_csv_file():
    df = pd.read_csv(ORIGINAL_DATA_PATH)

    print(df)

    return df.to_json()


def remove_null_values(ti):
    json_data = ti.xcom_pull(task_ids='read_csv_file')
    
    df = pd.read_json(json_data)

    df = df.dropna()

    dtypes = {
        'car_model_year': int,
        'mileage': int
    }

    df = df.astype(dtypes)

    print(df)

    df.to_csv(CLEANED_DATA_PATH, index=False)

    return df.to_json()


def determine_branch():
    choose = choice([True, False])
    
    if choose:
        return 'save_output_as_csv'
    else:
        return 'save_output_as_table'

def filter_by_new_car(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    filtered_df = df[df['condition'] == 'New']
    
    filtered_df.to_csv(OUTPUT_PATH.format('new_cars'), index=False)


def filter_by_used_car(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')
    df = pd.read_json(json_data)

    filtered_df = df[df['condition'] == 'Used']
    
    filtered_df.to_csv(OUTPUT_PATH.format('used_cars'), index=False)

def filter_by_certified_car(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')

    df = pd.read_json(json_data)

    filtered_df = df[df['condition'] == 'Certified Pre-Owned']
    
    filtered_df.to_csv(OUTPUT_PATH.format('certified_cars'), index=False)



with DAG(
    dag_id = 'data_processing_pipeline',
    description = 'Simple data processing pipeline',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once',
    tags = ['python', 'scaling', 'pipeline', 'celery'],
    template_searchpath = '/Users/jananiravi/airflow/sql_statements'
) as dag:

    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values
    )

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch
    )

    save_output_as_csv = EmptyOperator(
        task_id='save_output_as_csv'
    )

    save_output_as_table = EmptyOperator(
        task_id='save_output_as_table'
    )

    filter_by_new_car = PythonOperator(
        task_id='filter_by_new_car_csv',
        python_callable=filter_by_new_car
    )
    
    filter_by_used_car = PythonOperator(
        task_id='filter_by_used_car_csv',
        python_callable=filter_by_used_car
    )
    
    filter_by_certified_car = PythonOperator(
        task_id='filter_by_certified_car_csv',
        python_callable=filter_by_certified_car
    )

    create_table_new_car = PostgresOperator(
        task_id='create_table_new_car',
        postgres_conn_id='cars_postgres_conn',
        sql='create_table_car.sql',
        params={'table_name': 'new_car'}
    )

    create_table_used_car = PostgresOperator(
        task_id='create_table_used_car',
        postgres_conn_id='cars_postgres_conn',
        sql='create_table_car.sql',
        params={'table_name': 'used_car'}
    )

    create_table_certified_car = PostgresOperator(
        task_id='create_table_certified_car',
        postgres_conn_id='cars_postgres_conn',
        sql='create_table_car.sql',
        params={'table_name': 'certified_car'}
    )

    insert_data_new_car = PostgresOperator(
        task_id='insert_data_new_car',
        postgres_conn_id='cars_postgres_conn',
        sql='insert_car_data.sql',
        params={
            'table_name': 'new_car', 
            'csv_path': f"'{CLEANED_DATA_PATH}'", 
            'condition': "'New'"
        }
    )

    insert_data_used_car = PostgresOperator(
        task_id='insert_data_used_car',
        postgres_conn_id='cars_postgres_conn',
        sql='insert_car_data.sql',
        params={
            'table_name': 'used_car', 
            'csv_path': f"'{CLEANED_DATA_PATH}'", 
            'condition': "'Used'"
        }
    )

    insert_data_certified_car = PostgresOperator(
        task_id='insert_data_certified_car',
        postgres_conn_id='cars_postgres_conn',
        sql='insert_car_data.sql',
        params={
            'table_name': 'certified_car', 
            'csv_path': f"'{CLEANED_DATA_PATH}'", 
            'condition': "'Certified Pre-Owned'"
        }
    )

read_csv_file >> remove_null_values >> \
    determine_branch >> [save_output_as_csv, save_output_as_table]

save_output_as_csv >> \
    [filter_by_new_car, filter_by_used_car, filter_by_certified_car]

save_output_as_table >> \
    [create_table_new_car, create_table_used_car, create_table_certified_car]

create_table_new_car >> insert_data_new_car
create_table_used_car >> insert_data_used_car
create_table_certified_car >> insert_data_certified_car

---------------------------------------------------------------

# Go to the Airflow UI

# Show the Grid

# Show the graph (so we can see the pipeline)

# No need to run the DAG - we will run using Celery

---------------------------------------------------------------

# Now we'll run this using celery

# Here we will setup Celery with flower and rabbitmq to built production ready
# workflow.
# Note: flower is a webserver for Celery and rabbitmq is a message broker

# Installing RabbitMQ
# https://www.rabbitmq.com/install-homebrew.html

# In terminal
$ brew install rabbitmq
# ==> Caveats
# ==> rabbitmq
# Management Plugin enabled by default at http://localhost:15672


$ brew services restart rabbitmq

$ brew info rabbitmq

# Open up a new tab on the same browser you have the Airflow UI running

# > Goto browser at http://localhost:15672/

username: guest
password: guest

# Now lets create a new user called "loonycorn"

# Click on "Admin" from the navigation bar
Username: loonycorn
Password: password
Confirm Password: password
Tags: administrator (select this from the tags next to the box)

# Click "Add user"

# Now click on "loonycorn" user

# Click "Set Permission"
# Click "Set topic Permission"

# > Now logout off "guest" and login as "loonycorn"

username: loonycorn
password: password


-----------------------------------------------------------------------------

# Open airflow.cfg in Sublimetext

# Update this
executor = CeleryExecutor

# This should already be set
sql_alchemy_conn = postgresql://loonycorn:password@localhost:5432/airflow_metadata

# making celery point to rabbitmq server
broker_url = amqp://loonycorn:password@localhost:/

# Celery sends updates on airflow tasks
result_backend = db+postgresql://loonycorn:password@localhost:5432/airflow_metadata_db

# > Stop and restart the scheduler and webserver

--------------------------------------------------------------------------------

# Now lets setup "flower"

# In a new terminal

# Scroll upto "Celery components" section
$ airflow cheat-sheet

# This will start celery flower
$ airflow celery flower


# Open a 3rd tab on the same browser

# > In the browser goto http://localhost:5555/

# Click on each of the 3 tables - Dashboard, Tasks, Broker

# Observe there is no worker node present


--------------------------------------------------------------------------------


# In another terminal (in your virtual environment)


# Start a Celery worker node
$ celery -A airflow.executors.celery_executor worker

#  -------------- celery@Nawaz-MBP v5.2.7 (dawn-chorus)
# --- ***** -----
# -- ******* ---- Darwin-19.6.0-x86_64-i386-64bit 2023-02-07 09:41:42
# - *** --- * ---
# - ** ---------- [config]
# - ** ---------- .> app:         airflow.executors.celery_executor:0x7f95e90bddd0
# - ** ---------- .> transport:   amqp://loonycorn:**@localhost:5672//
# - ** ---------- .> results:     postgresql://loonycorn:**@localhost:5432/airflow_metadata
# - *** --- * --- .> concurrency: 16 (prefork)
# -- ******* ---- .> task events: OFF (enable -E to monitor tasks in this worker)
# --- ***** -----
#  -------------- [queues]
#                 .> default          exchange=default(direct) key=default



# Observe here "celery@Nawaz-MBP" is the celery worker name and the queue
# name is "default" we can check this in flower UI

# > Go back to http://localhost:5555/ and refresh

# Observe there is a worker node present now

# https://flower.readthedocs.io/en/latest/screenshots.html

# > Click on the worker node to get more information

# Click on "Queues" and show the default queue

---------------------------------------------------------------------

# Notes

# By using a Celery executor with RabbitMQ and Flower, you can take advantage
# of the following benefits:

# Parallel Processing: Airflow tasks can be executed in parallel across multiple
# worker nodes, increasing the efficiency and speed of your workflows.

# Scalability: As your workload increases, you can add more worker nodes to handle
# the increased load, making it easy to scale your Airflow setup.

# Task Monitoring: Flower provides a web-based interface for monitoring and
# managing Celery tasks in real-time, including viewing task status, progress,
# and detailed information about task execution.

# Task Distribution: RabbitMQ acts as a message broker, distributing tasks
# to worker nodes for processing. This allows for a flexible and scalable task
# distribution system, allowing you to add or remove worker nodes as needed.

---------------------------------------------------------------------

# Go to the Airflow UI

# > Toggle on "data_processing_pipeline"

# A random branch will be selected and executed

# > We can observe the "Gantt Chart" to see how the tasks have been executed

# > Goto flower UI and observe 6 have been processed

# Click on Tasks and show that there are 6 tasks here

# Click on the "UUID" of 2-3 tasks

# Note that they were all run by the same worker

# In the next demos we will see how tasks will be distributed among different workers.

# > We can trigger the DAG again and check in "Worker Name" >> "Tasks" in flower UI to see all the active, scheduled tasks

---------------------------------------------------------------------

# > Now stop the celery worker by clicking (ctrl+c)

# Here lets add 2 worker in the same machine ideally we have to do this in a
# seperate machine

# In terminal have two tables open

# terminal 1st tab

# Here we are creating a new worker with the worker name as celery_worker1
# and having the log level of info
$ celery -A airflow.executors.celery_executor worker -l info -n celery_worker1

#  -------------- celery@celery_worker1 v5.2.7 (dawn-chorus)
# --- ***** -----
# -- ******* ---- Darwin-19.6.0-x86_64-i386-64bit 2023-02-07 09:47:36
# - *** --- * ---
# - ** ---------- [config]
# - ** ---------- .> app:         airflow.executors.celery_executor:0x7fd7969020d0
# - ** ---------- .> transport:   amqp://loonycorn:**@localhost:5672//
# - ** ---------- .> results:     postgresql://loonycorn:**@localhost:5432/airflow_metadata
# - *** --- * --- .> concurrency: 16 (prefork)
# -- ******* ---- .> task events: OFF (enable -E to monitor tasks in this worker)
# --- ***** -----
#  -------------- [queues]
#                 .> default          exchange=default(direct) key=default


# Observe here "celery@celery_worker1" is the celery worker name and the queue
# name is "default" we can check this in flower UI


# terminal 2nd tab

# Here we are creating a new worker with the worker name as celery_worker2
# and having the log level of info
$ celery -A airflow.executors.celery_executor worker -l info -n celery_worker2

#  -------------- celery@celery_worker2 v5.2.7 (dawn-chorus)
# --- ***** -----
# -- ******* ---- Darwin-19.6.0-x86_64-i386-64bit 2023-02-07 09:47:46
# - *** --- * ---
# - ** ---------- [config]
# - ** ---------- .> app:         airflow.executors.celery_executor:0x7f8f5d7ffed0
# - ** ---------- .> transport:   amqp://loonycorn:**@localhost:5672//
# - ** ---------- .> results:     postgresql://loonycorn:**@localhost:5432/airflow_metadata
# - *** --- * --- .> concurrency: 16 (prefork)
# -- ******* ---- .> task events: OFF (enable -E to monitor tasks in this worker)
# --- ***** -----
#  -------------- [queues]
#                 .> default          exchange=default(direct) key=default


# Observe here "celery@celery_worker2" is the celery worker name and the queue
# name is "default" we can check this in flower UI



# > Go back to http://localhost:5555/ and refresh

# Observe here there are now 2 workers (the older one will be offline)


# > Trigger "data_processing_pipeline"

# > We can observe the "Gantt Chart" to see how the tasks have been executed

# > Goto flower UI and observe tasks have been processed

# Click on the Tasks tab

# > Observe here 3 tasks have been processed by celery_worker1 and the
# other 3 have been processed bu celery_worker2


---------------------------------------------------------


# Assigning specific queues to tasks

# > Stop the 2 celery works in the terminal by clicking (ctrl+c)

# In terminal

# tab 1
$ celery -A airflow.executors.celery_executor worker -l info -n celery_worker1 -Q queueA

#  -------------- celery@celery_worker1 v5.2.7 (dawn-chorus)
# --- ***** -----
# -- ******* ---- Darwin-19.6.0-x86_64-i386-64bit 2023-02-07 10:55:21
# - *** --- * ---
# - ** ---------- [config]
# - ** ---------- .> app:         airflow.executors.celery_executor:0x7fd1f38bfe50
# - ** ---------- .> transport:   amqp://loonycorn:**@localhost:5672//
# - ** ---------- .> results:     postgresql://loonycorn:**@localhost:5432/airflow_metadata
# - *** --- * --- .> concurrency: 16 (prefork)
# -- ******* ---- .> task events: OFF (enable -E to monitor tasks in this worker)
# --- ***** -----
#  -------------- [queues]
#                 .> queueA           exchange=queueA(direct) key=queueA


# Observe here "celery@celery_worker1" is the celery worker name and the queue
# name is "queueA" we can check this in flower UI


# tab 2
$ celery -A airflow.executors.celery_executor worker -l info -n celery_worker2 -Q queueB

#  -------------- celery@celery_worker2 v5.2.7 (dawn-chorus)
# --- ***** -----
# -- ******* ---- Darwin-19.6.0-x86_64-i386-64bit 2023-02-07 10:55:31
# - *** --- * ---
# - ** ---------- [config]
# - ** ---------- .> app:         airflow.executors.celery_executor:0x7fde360bffd0
# - ** ---------- .> transport:   amqp://loonycorn:**@localhost:5672//
# - ** ---------- .> results:     postgresql://loonycorn:**@localhost:5432/airflow_metadata
# - *** --- * --- .> concurrency: 16 (prefork)
# -- ******* ---- .> task events: OFF (enable -E to monitor tasks in this worker)
# --- ***** -----
#  -------------- [queues]
#                 .> queueB           exchange=queueB(direct) key=queueB

# Observe here "celery@celery_worker2" is the celery worker name and the queue
# name is "queueB" we can check this in flower UI


# Just replace the code

    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file,
        queue='queueA'
    )

    remove_null_values = PythonOperator(
        task_id='remove_null_values',
        python_callable=remove_null_values,
        queue='queueA'
    )

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',
        python_callable=determine_branch,
        queue='queueB'
    )

    save_output_as_csv = EmptyOperator(
        task_id='save_output_as_csv',
        queue='queueA'
    )

    save_output_as_table = EmptyOperator(
        task_id='save_output_as_table',
        queue='queueB'
    )

    filter_by_new_car = PythonOperator(
        task_id='filter_by_new_car_csv',
        python_callable=filter_by_new_car,
        queue='queueA'
    )
    
    filter_by_used_car = PythonOperator(
        task_id='filter_by_used_car_csv',
        python_callable=filter_by_used_car,
        queue='queueB'
    )
    
    filter_by_certified_car = PythonOperator(
        task_id='filter_by_certified_car_csv',
        python_callable=filter_by_certified_car,
        queue='queueA'
    )

    create_table_new_car = PostgresOperator(
        task_id='create_table_new_car',
        postgres_conn_id='cars_postgres_conn',
        sql='create_table_car.sql',
        params={'table_name': 'new_car'},
        queue='queueA'
    )

    create_table_used_car = PostgresOperator(
        task_id='create_table_used_car',
        postgres_conn_id='cars_postgres_conn',
        sql='create_table_car.sql',
        params={'table_name': 'used_car'},
        queue='queueB'
    )

    create_table_certified_car = PostgresOperator(
        task_id='create_table_certified_car',
        postgres_conn_id='cars_postgres_conn',
        sql='create_table_car.sql',
        params={'table_name': 'certified_car'},
        queue='queueB'
    )

    insert_data_new_car = PostgresOperator(
        task_id='insert_data_new_car',
        postgres_conn_id='cars_postgres_conn',
        sql='insert_car_data.sql',
        params={
            'table_name': 'new_car', 
            'csv_path': f"'{CLEANED_DATA_PATH}'", 
            'condition': "'New'"
        },
        queue='queueA'
    )

    insert_data_used_car = PostgresOperator(
        task_id='insert_data_used_car',
        postgres_conn_id='cars_postgres_conn',
        sql='insert_car_data.sql',
        params={
            'table_name': 'used_car', 
            'csv_path': f"'{CLEANED_DATA_PATH}'", 
            'condition': "'Used'"
        },
        queue='queueB'
    )

    insert_data_certified_car = PostgresOperator(
        task_id='insert_data_certified_car',
        postgres_conn_id='cars_postgres_conn',
        sql='insert_car_data.sql',
        params={
            'table_name': 'certified_car', 
            'csv_path': f"'{CLEANED_DATA_PATH}'", 
            'condition': "'Certified Pre-Owned'"
        },
        queue='queueB'
    )


# Trigger the "data_processing_pipeline" 

# We can observe the "Gantt Chart" to see how the tasks have been executed

# Goto flower UI and observe tasks have been processed

# > Observe that the tasks are split between the two workers

# Go to RabbitMQ

# Click on queues

# Note that in addition to the "default" queue we have "queueA" and "queueB"













