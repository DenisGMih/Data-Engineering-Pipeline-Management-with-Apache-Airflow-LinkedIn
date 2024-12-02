from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import pandas as pd

from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable


default_args = {
   'owner': 'loonycorn'
}

CLEAN_CUSTOMERS_DATA_PATH = '/Users/jananiravi/airflow/output/clean_customers.csv'
CLEAN_PURCHASES_DATA_PATH = '/Users/jananiravi/airflow/output/clean_purchases.csv'
JOINING_DATA_PATH = '/Users/jananiravi/airflow/output/joined_data.csv'

clean_customers_dataset = Dataset(CLEAN_CUSTOMERS_DATA_PATH)
clean_purchases_dataset = Dataset(CLEAN_PURCHASES_DATA_PATH)


def reading_clean_customers_data():
    df = pd.read_csv(CLEAN_CUSTOMERS_DATA_PATH)

    print(df)

    return df.to_json()

def reading_clean_purchases_data():
    df = pd.read_csv(CLEAN_PURCHASES_DATA_PATH)

    print(df)

    return df.to_json()


def joining_the_data(ti):
    json_data = ti.xcom_pull(task_ids='reading_clean_customers_data')
    df_customers = pd.read_json(json_data)

    json_data = ti.xcom_pull(task_ids='reading_clean_purchases_data')
    df_purchases = pd.read_json(json_data)

    joined_data = pd.merge(df_customers, df_purchases, left_on='id', right_on='customer_id')

    joined_data.drop('id_x', axis=1, inplace=True)
    joined_data.drop('id_y', axis=1, inplace=True)
    joined_data.drop('customer_id', axis=1, inplace=True)

    joined_data.drop(['ip_address', 'city'], axis=1, inplace=True)

    joined_data['total_price'] = joined_data['price'] * joined_data['quantity']

    print(joined_data)

    return joined_data.to_json()


def saving_joined_data(ti):
    json_data = ti.xcom_pull(task_ids='joining_the_data')

    df = pd.read_json(json_data)

    df.to_csv(JOINING_DATA_PATH, mode='w', header=True, index=False)



with DAG(
    dag_id = 'consumer_pipeline_join',
    description = 'Executing a data-aware consumer pipeline with datasets',
    default_args = default_args,
    start_date = days_ago(1),
    schedule = [clean_customers_dataset, clean_purchases_dataset],
    tags = ['python', 'datasets', 'consumer', 'join']
) as dag:


    reading_clean_customers_data = PythonOperator(
        task_id = 'reading_clean_customers_data',
        python_callable = reading_clean_customers_data
    )

    reading_clean_purchases_data = PythonOperator(
        task_id = 'reading_clean_purchases_data',
        python_callable = reading_clean_purchases_data
    )
    
    joining_the_data = PythonOperator(
        task_id = 'joining_the_data',
        python_callable = joining_the_data
    )

    saving_joined_data = PythonOperator(
        task_id = 'saving_joined_data',
        python_callable = saving_joined_data
    )

    [reading_clean_customers_data, reading_clean_purchases_data] \
        >> joining_the_data >> saving_joined_data


